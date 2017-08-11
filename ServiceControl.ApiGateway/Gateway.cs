using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Microsoft.Net.Http.Server;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

enum MessageStatus
{
    Failed = 1,
    RepeatedFailure = 2,
    Successful = 3,
    ResolvedSuccessfully = 4,
    ArchivedFailure = 5
}

class Message
{
    public string message_id { get; set; }
    public string message_type { get; set; }
    public MessageStatus status { get; set; }
    public DateTime processed_at { get; set; }
    public DateTime time_sent { get; set; }
    public TimeSpan? critical_time { get; set; }
    public TimeSpan? processing_time { get; set; }
    public TimeSpan? delivery_time { get; set; }
}

class PartialQueryResult
{
    public int StartOffset { get; set; }
    public JArray Messages { get; set; }
    public int TotalCount { get; set; }
}

class Gateway
{
    CancellationTokenSource stopTokenSource;
    WebListener listener;
    Task workerTask;
    ConcurrentDictionary<Task, Task> runningReceiveTasks = new ConcurrentDictionary<Task, Task>();
    string urlPrefix;
    string primaryPrefix;
    string[] secondaries;
    HttpClient client;
    JsonSerializer serializer;

    static Regex[] aggregateUrls = {
        new Regex("^/messages/?$"),
        new Regex("^/endpoints/.*/messages/?$"),
        new Regex("^/messages/search"),
        new Regex("^/endpoints/.*/messages/search"),
        new Regex("^/conversations/")
    };

    public Gateway(string urlPrefix, string primaryPrefix, string[] secondaries)
    {
        this.urlPrefix = urlPrefix;
        this.primaryPrefix = primaryPrefix;
        this.secondaries = secondaries;
        serializer = new JsonSerializer();
    }

    public void Start()
    {
        var settings = new WebListenerSettings();
        settings.UrlPrefixes.Add((string)urlPrefix);
        stopTokenSource = new CancellationTokenSource();
        listener = new WebListener(settings);
        listener.Start();
        workerTask = Task.Run(Listen);
        client = new HttpClient();
    }

    public async Task Stop()
    {
        listener.Dispose();
        stopTokenSource.Cancel();
        await workerTask.ConfigureAwait(false);
    }

    async Task Listen()
    {
        while (!stopTokenSource.IsCancellationRequested)
        {
            try
            {
                await Accept().ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // For graceful shutdown purposes
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Request processing failed: {ex}");
            }
        }
    }

    async Task Accept()
    {
        while (!stopTokenSource.IsCancellationRequested)
        {
            var context = await listener.AcceptAsync().ConfigureAwait(false);

            if (stopTokenSource.IsCancellationRequested)
            {
                return;
            }

            var receiveTask = ReceiveMessage(context);

            runningReceiveTasks.TryAdd(receiveTask, receiveTask);

            // We insert the original task into the runningReceiveTasks because we want to await the completion
            // of the running receives. ExecuteSynchronously is a request to execute the continuation as part of
            // the transition of the antecedents completion phase. This means in most of the cases the continuation
            // will be executed during this transition and the antecedent task goes into the completion state only
            // after the continuation is executed. This is not always the case. When the TPL thread handling the
            // antecedent task is aborted the continuation will be scheduled. But in this case we don't need to await
            // the continuation to complete because only really care about the receive operations. The final operation
            // when shutting down is a clear of the running tasks anyway.
#pragma warning disable 4014
            receiveTask.ContinueWith((t, state) =>
#pragma warning restore 4014
            {
                var receiveTasks = (ConcurrentDictionary<Task, Task>)state;
                Task toBeRemoved;
                receiveTasks.TryRemove(t, out toBeRemoved);
            }, runningReceiveTasks, TaskContinuationOptions.ExecuteSynchronously);
        }
    }

    Tuple<JArray, int[]> AggregateResults(int pageSize, string sort, string sortDirection, string browseDirection, PartialQueryResult[] partialResults)
    {
        var heads = new Message[partialResults.Length];
        var localOffsets = partialResults.Select(x => x.StartOffset % pageSize).ToArray();
        var newOffsets = partialResults.Select(x => x.StartOffset).ToArray();
        Func<Message, IComparable> keySelector = m => m.time_sent;
        switch (sort)
        {
            case "id":
            case "message_id":
                keySelector = m => m.message_id;
                break;

            case "message_type":
                keySelector = m => m.message_type;
                break;

            case "critical_time":
                keySelector = m => m.critical_time;
                break;

            case "delivery_time":
                keySelector = m => m.delivery_time;
                break;

            case "processing_time":
                keySelector = m => m.processing_time;
                break;

            case "processed_at":
                keySelector = m => m.processed_at;
                break;

            case "status":
                keySelector = m => m.status;
                break;
        }

        var results = new List<JObject>();
        for (var i = 0; i < pageSize; i++)
        {
            for (var headIndex = 0; headIndex < heads.Length; headIndex++)
            {
                //Replenish
                if (heads[headIndex] == null)
                {
                    if (localOffsets[headIndex] < partialResults[headIndex].Messages.Count && localOffsets[headIndex] >= 0)
                    {
                        var messageToken = partialResults[headIndex].Messages[localOffsets[headIndex]];
                        heads[headIndex] = messageToken.ToObject<Message>(serializer);
                    }
                }
            }
            //Find one with smallest/largest value depending on sort direction
            Func<int, bool> comparison;
            if (sortDirection == "asc" && browseDirection == "right" || sortDirection == "desc" && browseDirection == "left")
            {
                comparison = x => x < 0;
            }
            else
            {
                comparison = x => x > 0;
            }
            var next = Min(heads, keySelector, comparison);
            if (next < 0) //No more messages
            {
                break;
            }
            results.Add((JObject)partialResults[next].Messages[localOffsets[next]]);

            var offsetIncrement = browseDirection == "right" ? 1 : -1;

            localOffsets[next] += offsetIncrement;
            newOffsets[next] += offsetIncrement;
            heads[next] = null;
        }
        //var newOffsets = partialResults.Select(x => x.StartOffset / pageSize).Zip(localOffsets, (x, y) => x + y).ToArray();
        return Tuple.Create(new JArray(results.ToArray()), newOffsets);
    }

    static int Min<T>(Message[] messages, Func<Message, T> sortOrder, Func<int, bool> comparison)
        where T : IComparable
    {
        IComparable minValue = default(T);
        var anyValue = false;
        var minIndex = -1;

        for (var i = 0; i < messages.Length; i++)
        {
            if (messages[i] == null)
            {
                continue;
            }
            var value = sortOrder(messages[i]);
            if (!anyValue || comparison(value.CompareTo(minValue)))
            {
                minValue = value;
                minIndex = i;
            }
            anyValue = true;
        }
        return minIndex;
    }

    async Task<PartialQueryResult[]> DistributeQuery(int pageSize, string sort, string direction, int[] offsetArray, string urlSuffix)
    {
        var queryTasks = secondaries.Zip(offsetArray, (url, offset) => new { url, offset })
            .Select(x => new {x.offset, request = new HttpRequestMessage(HttpMethod.Get, BuildUrl(x.url, urlSuffix, x.offset, pageSize, sort, direction)) })
            .Select(async x =>
            {
                var rawResponse = await client.SendAsync(x.request);
                var result = await ParseResult(rawResponse, x.offset);
                return result;
            })
            .ToArray();

        var responses = await Task.WhenAll(queryTasks);
        return responses;
    }

    async Task<PartialQueryResult> ParseResult(HttpResponseMessage responseMessage, int index)
    {
        var responseStream = await responseMessage.Content.ReadAsStreamAsync();
        var jsonReader = new JsonTextReader(new StreamReader(responseStream));
        var messages = await JArray.LoadAsync(jsonReader);

        var totalCount = responseMessage.Headers.GetValues("Total-Count").Select(int.Parse).Cast<int?>().FirstOrDefault() ?? -1;

        return new PartialQueryResult
        {
            Messages = messages,
            TotalCount = totalCount,
            StartOffset = index
        };
    }

    static Uri BuildUrl(string prefix, string suffix, int offset, int pageSize, string sort, string direction)
    {
        var pageNumber = offset / pageSize + 1;
        var uri = new Uri($"{prefix}{suffix}?page={pageNumber}&per_page={pageSize}&direction={direction}&sort={sort}");
        return uri;
    }

    Task ReceiveMessage(RequestContext context)
    {
        return RunWorkerTask(async state =>
        {
            try
            {
                if (aggregateUrls.Any(x => x.IsMatch(context.Request.Path)))
                {
                    await DistributeToSecondaries(context);
                }
                else
                {
                    await ForwardToPrimary(context);
                }
            }
            catch (OperationCanceledException)
            {
                // Intentionally ignored
            }
            catch (Exception ex)
            {
                context.Abort();
            }
        }, this);
    }

    async Task DistributeToSecondaries(RequestContext context)
    {
        var queryStringParsed = context.Request.QueryString.TrimStart('?')
            .Split(new[] {'&'}, StringSplitOptions.RemoveEmptyEntries)
            .Select(kvp => kvp.Split(new[] {'='}, StringSplitOptions.RemoveEmptyEntries))
            .ToDictionary(x => x[0], x => x.Length > 1 ? x[1] : null);

        var offsetValues = new int[secondaries.Length];
        if (queryStringParsed.TryGetValue("offsets", out string offsetsParam) && offsetsParam != null)
        {
            var offsetsSplit = offsetsParam.Split(new[] {','}, StringSplitOptions.RemoveEmptyEntries);
            offsetValues = offsetsSplit.Select(int.Parse).ToArray();
        }
        var pageSize = 50; //SC default value
        if (queryStringParsed.TryGetValue("per_page", out string pageSizeString) && pageSizeString != null)
        {
            pageSize = int.Parse(pageSizeString);
        }

        string sort;
        if (!queryStringParsed.TryGetValue("sort", out sort) || sort == null)
        {
            sort = "time_sent";
        }
        string direction;
        if (!queryStringParsed.TryGetValue("direction", out direction) || direction == null)
        {
            direction = "desc";
        }

        string browseDirection;
        if (!queryStringParsed.TryGetValue("browse_direction", out browseDirection)|| browseDirection == null)
        {
            browseDirection = "right";
        }

        var pageNumber = 1;
        if (queryStringParsed.TryGetValue("page", out string pageNumberText) && pageNumberText != null)
        {
            pageNumber = int.Parse(pageNumberText);
        }

        var partialResults = await DistributeQuery(pageSize, sort, direction, offsetValues, context.Request.Path);
        var combinedResults = AggregateResults(pageSize, sort, direction, browseDirection, partialResults);
        var newOffsets = string.Join(",", combinedResults.Item2);
        var totalCount = partialResults.Select(x => x.TotalCount).Sum();

        context.Response.Headers.Add("Access-Control-Allow-Headers", new StringValues(new[] { "Origin", "X-Requested-With", "Content-Type", "Accept" }));
        context.Response.Headers.Add("Access-Control-Allow-Methods", new StringValues(new[] { "POST", "GET", "PUT", "DELETE", "PATCH", "OPTIONS" }));
        context.Response.Headers.Add("Access-Control-Allow-Origin", "*");
        context.Response.Headers.Add("Access-Control-Expose-Headers", new StringValues(new[] { "ETag", "Last-Modified", "Link", "Total-Count", "X-Particular-Version" }));

        context.Response.Headers.Add("Cache-Control", "private, max-age=0");
        context.Response.Headers.Add("X-Particular-Version", "1.41.0"); //Get it from the reponse
        context.Response.Headers.Add("Total-Count", totalCount.ToString());
        context.Response.Headers.Add("Page-Size", pageSize.ToString());
        context.Response.Headers.Add("Page-Number", pageNumber.ToString());

        context.Response.ContentType = "application/json; charset=utf-8";

        var lastPageOffsets = partialResults.Select(x => x.TotalCount - 1);
        var pageCount = totalCount / pageSize + 1;

        var links = new List<string>();
        if (pageNumber > 1)
        {
            links.Add(GenerateLink(context, "prev", pageSize, browseDirection == "left" ? newOffsets : string.Join(",", offsetValues.Select(o => o - 1)), direction, sort, "left", pageNumber - 1));
            links.Add(GenerateLink(context, "first", pageSize, "", direction, sort, "right", 1));
        }
        if (pageNumber < pageCount)
        {
            links.Add(GenerateLink(context, "next", pageSize, browseDirection == "right" ? newOffsets : string.Join(",", offsetValues.Select(o => o + 1)), direction, sort, "right", pageNumber + 1));
            links.Add(GenerateLink(context, "last", pageSize, string.Join(",", lastPageOffsets), direction, sort, "left", pageCount));
        }

        context.Response.Headers.Add("Link", string.Join(",", links));

        using (var writer = new JsonTextWriter(new StreamWriter(context.Response.Body)))
        {
            await combinedResults.Item1.WriteToAsync(writer);
        }
        context.Dispose();
    }

    static string GenerateLink(RequestContext context, string rel, int pageSize, string newOffsets, string direction, string sort, string browseDirection, int pageNumber)
    {
        return $"<{context.Request.Path.TrimStart('/')}?per_page={pageSize}&offsets={newOffsets}&direction={direction}&sort={sort}&browse_direction={browseDirection}&page={pageNumber}>; rel=\"{rel}\"";
    }

    async Task ForwardToPrimary(RequestContext context)
    {
        var request = new HttpRequestMessage
        {
            Method = new HttpMethod(context.Request.Method),
            RequestUri = new Uri(primaryPrefix + context.Request.Path)
        };
        var response = await client.SendAsync(request, context.DisconnectToken);

        foreach (var responseHeader in response.Headers)
        {
            if (responseHeader.Key == "Transfer-Encoding")
            {
                continue;
            }
            context.Response.Headers.Add(responseHeader.Key, new StringValues(responseHeader.Value.ToArray()));
        }
        foreach (var contentHeader in response.Content.Headers)
        {
            context.Response.Headers.Add(contentHeader.Key, new StringValues(contentHeader.Value.ToArray()));
        }

        context.Response.StatusCode = (int) response.StatusCode;
        await response.Content.CopyToAsync(context.Response.Body).ConfigureAwait(false);
        context.Dispose();
    }

    static Task RunWorkerTask(Func<object, Task> func, object state)
        => Task.Factory.StartNew(func, state, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default).Unwrap();

}