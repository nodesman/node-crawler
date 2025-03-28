export type GlobalOnlyOptions = {
    /**
     * Global Only option.
     * @default 10
     * @description The maximum number of requests that can be sent simultaneously.
     * @example If the value is 10, the crawler will send at most 10 requests at the same time.
     * Note: The maxConnections(> 1) will be valid only if the global ratelimit is set to be 0.
     */
    maxConnections: number;
    /**
     * Global Only option.
     * @default 10
     * @description The number of levels of priority. Can be only assigned at the beginning.
     */
    priorityLevels: number;
    /**
     * Global Only option.
     * @default 0
     * @description The default priority of the tasks. Can be only assigned at the beginning.
     * @example 1000 means 1000 milliseconds delay between after the first request.
     */
    rateLimit: number;
    /**
     * Global Only option.
     * @default false
     * @description If true, the crawler will skip duplicate tasks.
     * @example If the task is already in the queue, the crawler will not add it again.
     */
    skipDuplicates: boolean;
    /**
     * Global Only option.
     * @default false
     * @description If true, the crawler will dynamically reallocate the tasks within the queue blocked due to header blocking to other queues.
     */
    homogeneous: boolean;
    /**
     * Global Only option.
     * @default undefined
     * @description If passed, the crawler will rotate the user agent for each request. The "userAgents" option must be an array if activated.
     */
    userAgents?: string | string[];
    /**
     * Global Only option.
     * @default false
     * @description If true, the crawler will mute all warning and error messages. The request error will be still thrown.
     */
    silence?: boolean;
};

export type RequestOptions = {
    forceUTF8?: boolean;
    /**
     * crawlerOption
     * @default true
     * @description If true, the crawler will use the cheerio library to parse the HTML content.
     * @see cheerio.load()
     * @example If inject successfully, the response object will have "$" property, which is a function to use jQuery.
     */
    jQuery?: boolean;
    /**
     * @deprecated
     * @description Please use "encoding" instead.
     */
    incomingEncoding?: string | null;
    /**
     * @default "utf8"
     * @description The encoding of the response body.
     */
    encoding?: string | null;
    /**
     * @default 0
     * @description rateLimiter ID
     */
    rateLimiterId?: number;
    /**
     * @default 2
     * @description The retry count of the request.
     */
    retries?: number;
    /**
     * @default 3000
     * @description The interval between retries in milliseconds.
     */
    retryInterval?: number;
    /**
     * @default 20000
     * @description The global timeout of the request in milliseconds.
     */
    timeout?: number;
    priority?: number;
    seenreq?: any;

    method?: string;
    skipEventRequest?: boolean;
    html?: boolean;
    proxies?: string[];
    proxy?: string;
    http2?: boolean;
    body?: string | Record<string, unknown>;
    headers?: Record<string, unknown>;
    agent?: any;

    /**
     * @deprecated Please use "url" instead.
     */
    uri?: string | ((urlFn: (url: string) => void) => void);
    url?: string | ((urlFn: (url: string) => void) => void);

    /**
     * @deprecated Please use "searchParams" instead.
     */
    qs?: string | Record<string, unknown>;
    searchParams?: Record<string, unknown>;

    /**
     * @deprecated Please use "rejectUnauthorized" instead.
     */
    strictSSL?: boolean;
    /**
     * @description If false, the crawler will ignore SSL certificate errors.
     * @default true
     */
    rejectUnauthorized?: boolean;

    /**
     * @deprecated Please use "decompress" instead.
     */
    gzip?: boolean;
    decompress?: boolean;

    /**
     * @deprecated Please use "cookieJar" instead.
     * @see tough-cookie https://github.com/sindresorhus/got/blob/main/documentation/migration-guides/request.md
     */
    jar?: object;
    cookieJar?: object;

    /**
     * @default false
     *
     * If true, the crawler will parse the response body as JSON.
     * This will set 'jQuery' to false.
     */
    isJson?: boolean;

    referer?: string;
    userParams?: unknown;
    /**
     * @deprecated Please use "parseJson" instead.
     */
    jsonReviver?: (text: string) => unknown;
    parseJson?: (text: string) => unknown;

    /**
     * @deprecated Please use "stringifyJson" instead.
     */
    jsonReplacer?: (object: unknown) => string;
    stringifyJson?: (object: unknown) => string;

    preRequest?: (options: RequestOptions, done?: (error?: Error | null) => void) => void;
    release?: () => void;
    callback?: (error: unknown, response: CrawlerResponse, done?: unknown) => void;
    /**
     * @default undefined
     * @description The maximum size of a response body in bytes.
     * If set, the crawler will attempt to prevent downloading files larger than this limit.
     * It uses a HEAD request first (if possible) and monitors download progress.
     */
    maxSizeBytes?: number;

    /**
     * @default false
     * @description If true and maxSizeBytes is set, the crawler will reject the request
     * during the initial HEAD check if the Content-Length header is missing.
     * If false, it will proceed to download and monitor stream progress.
     */
    rejectOnMissingContentLength?: boolean;
};

export type RequestConfig = string | RequestOptions | RequestOptions[];
export type CrawlerOptions = Partial<GlobalOnlyOptions> & RequestOptions;
export type CrawlerResponse = any;