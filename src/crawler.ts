import { EventEmitter } from "events";
import { Cluster } from "./rateLimiter/index.js";
import { isBoolean, isFunction, setDefaults, flattenDeep, lowerObjectKeys, isNumber } from "./lib/utils.js";
import { getValidOptions, alignOptions, getCharset } from "./options.js";
import { getLogger } from "./logger.js";
import type { CrawlerOptions, RequestOptions, RequestConfig, CrawlerResponse } from "./types/crawler.js";
import { load } from "cheerio";
import got from "got";
import seenreq from "seenreq";
import iconv from "iconv-lite";
import { Buffer } from 'buffer';

// @todo: remove seenreq dependency
const log = getLogger();

interface CustomError extends Error {
    code?: string;
}

class Crawler extends EventEmitter {
    private _limiters: Cluster;
    private _UAIndex = 0;
    private _proxyIndex = 0;

    public options: CrawlerOptions;
    public seen: any;

    constructor(options?: CrawlerOptions) {
        super();
        const defaultOptions: CrawlerOptions = {
            maxConnections: 10,
            rateLimit: 0,
            priorityLevels: 10,
            skipDuplicates: false,
            homogeneous: false,
            method: "GET",
            forceUTF8: false,
            jQuery: true,
            priority: 5,
            retries: 2,
            retryInterval: 3000,
            timeout: 20000,
            isJson: false,
            silence: false,
        };
        this.options = { ...defaultOptions, ...options };
        if (this.options.rateLimit! > 0) {
            this.options.maxConnections = 1;
        }
        if (this.options.silence) {
            log.settings.minLevel = 7;
        }

        this._limiters = new Cluster({
            maxConnections: this.options.maxConnections!,
            rateLimit: this.options.rateLimit!,
            priorityLevels: this.options.priorityLevels!,
            defaultPriority: this.options.priority!,
            homogeneous: this.options.homogeneous,
        });

        this.seen = new seenreq(this.options.seenreq);
        this.seen
            .initialize()
            .then(() => {
                log.debug("seenreq initialized");
            })
            .catch((error: unknown) => {
                log.error(error);
            });
        this.on("_release", () => {
            log.debug(`Queue size: ${this.queueSize}`);
            if (this._limiters.empty) this.emit("drain");
        });
    }

    private _detectHtmlOnHeaders = (headers: Record<string, unknown>): boolean => {
        const contentType = headers["content-type"] as string;
        if (/xml|html/i.test(contentType)) return true;
        return false;
    };

    private _schedule = (options: CrawlerOptions): void => {
        this.emit("schedule", options);
        this._limiters
            .getRateLimiter(options.rateLimiterId)
            .submit(options.priority as number, (done, rateLimiterId) => {
                options.release = () => {
                    done();
                    this.emit("_release");
                };
                options.callback = options.callback || options.release;

                if (rateLimiterId) {
                    this.emit("limiterChange", options, rateLimiterId);
                }

                if (options.html) {
                    options.url = options.url ?? "";
                    this._handler(null, options, { body: options.html, headers: { "content-type": "text/html" } });
                } else {
                    options.url = options.url ?? options.uri;
                    if (typeof options.url === "function") {
                        options.url((url: string) => {
                            options.url = url;
                            this._execute(options);
                        });
                    } else {
                        delete options.uri;
                        this._execute(options);
                    }
                }
            });
    };

    private _execute = async (options: CrawlerOptions): Promise<CrawlerResponse> => {
        if (options.proxy) log.debug(`Using proxy: ${options.proxy}`);
        else if (options.proxies) log.debug(`Using proxies: ${options.proxies}`);

        options.headers = options.headers ?? {};
        options.headers = lowerObjectKeys(options.headers);

        if (options.forceUTF8 || options.isJson) options.encoding = "utf8";

        if (Array.isArray(options.userAgents)) {
            this._UAIndex = this._UAIndex % options.userAgents.length;
            options.headers["user-agent"] = options.userAgents[this._UAIndex];
            this._UAIndex++;
        } else {
            options.headers["user-agent"] = options.headers["user-agent"] ?? options.userAgents;
        }

        if (!options.proxy && Array.isArray(options.proxies)) {
            this._proxyIndex = this._proxyIndex % options.proxies.length;
            options.proxy = options.proxies[this._proxyIndex];
            this._proxyIndex++;
        }

        const maxSizeBytes = options.maxSizeBytes;
        let preliminaryCheckPassed = true;
        let checkError: CustomError | null = null;

        if (typeof maxSizeBytes === 'number' && maxSizeBytes > 0) {
            log.debug(`[Size Check] Max size set to ${maxSizeBytes} bytes for ${options.url}`);

            // Perform HEAD request first if possible to check Content-Length
            try {
                log.debug(`[Size Check] Performing HEAD request for ${options.url}`);
                const headOptions = alignOptions({
                    ...options,
                    method: 'HEAD',
                    timeout: options.timeout ?? 5000, // Use specific timeout for HEAD
                    retries: 0 // Disable retries for the HEAD check
                });

                const headResponse = await got(headOptions);
                const contentLength = headResponse.headers['content-length'];
                log.debug(`[Size Check] HEAD Content-Length for ${options.url}: ${contentLength}`);

                if (contentLength) {
                    const fileSize = parseInt(contentLength as string, 10);
                    if (!isNaN(fileSize) && fileSize > maxSizeBytes) {
                        preliminaryCheckPassed = false;
                        checkError = new Error(`File size ${fileSize} (from HEAD Content-Length) exceeds limit of ${maxSizeBytes} bytes`);
                        checkError.code = 'ERR_FILE_TOO_LARGE_HEAD';
                        log.warn(`[Size Check] Aborting (HEAD): ${options.url} - ${checkError.message}`);
                    }
                } else if (options.rejectOnMissingContentLength) {
                    preliminaryCheckPassed = false;
                    checkError = new Error(`Content-Length header missing in HEAD response and rejectOnMissingContentLength is true`);
                    checkError.code = 'ERR_MISSING_CONTENT_LENGTH_HEAD';
                    log.warn(`[Size Check] Aborting (HEAD): ${options.url} - ${checkError.message}`);
                } else {
                     log.debug(`[Size Check] HEAD Content-Length missing for ${options.url}, proceeding to GET/STREAM.`);
                }

            } catch (headError: any) {
                // Log HEAD errors but proceed to attempt the GET/STREAM by default
                log.warn(`[Size Check] HEAD request failed for ${options.url}: ${headError.message}. Proceeding to GET/STREAM.`);
                // Optionally, could abort here by setting preliminaryCheckPassed = false
                // preliminaryCheckPassed = false;
                // checkError = new Error(`HEAD request failed: ${headError.message}`);
                // checkError.code = 'ERR_HEAD_REQUEST_FAILED';
            }
        }

        // If preliminary check failed, go directly to handler with the error
        if (!preliminaryCheckPassed) {
            return this._handler(checkError, options);
        }

        const gotOptions = alignOptions(options);

        // Function to perform the request, potentially monitoring stream size
        const requestFn = async (): Promise<CrawlerResponse> => {
            if (options.skipEventRequest !== true) {
                this.emit("request", options);
            }

            // Use streaming only if a size limit is active
            if (typeof maxSizeBytes === 'number' && maxSizeBytes > 0) {
                return new Promise((resolve) => { // Wrap stream logic in a Promise
                    const dataChunks: Buffer[] = [];
                    let receivedBytes = 0;
                    let responseHeaders: Record<string, unknown> | null = null;
                    let statusCode: number | null = null;
                    let requestAborted = false;

                    log.debug(`[Size Check] Starting GET request with stream monitoring for ${options.url}`);
                    const stream = got.stream(gotOptions);

                    const abortRequest = (error: Error) => {
                        if (!requestAborted) {
                            requestAborted = true;
                            log.warn(`[Size Check] Aborting stream for ${options.url}: ${error.message}`);
                            stream.destroy(error);
                            resolve(this._handler(error, options));
                        }
                    };

                    stream.on('response', (response: any) => {
                        responseHeaders = response.headers;
                        statusCode = response.statusCode;
                        log.debug(`[Size Check] Stream response received for ${options.url} - Status: ${statusCode}`);

                        const contentLength = responseHeaders?.['content-length'];
                        if (contentLength) {
                            const fileSize = parseInt(contentLength as string, 10);
                            if (!isNaN(fileSize) && fileSize > maxSizeBytes!) {
                                const error = new Error(`File size ${fileSize} (from GET Content-Length) exceeds limit of ${maxSizeBytes} bytes`);
                                (error as CustomError).code = 'ERR_FILE_TOO_LARGE_RESPONSE_HEADER';
                                abortRequest(error);
                                return;
                            }
                        }
                    });

                    stream.on('data', (chunk: Buffer) => {
                        if (requestAborted) return;

                        receivedBytes += chunk.length;
                        dataChunks.push(chunk);

                        if (receivedBytes > maxSizeBytes!) {
                            const error = new Error(`Downloaded data size (${receivedBytes} bytes) exceeds limit of ${maxSizeBytes} bytes`);
                            (error as CustomError).code = 'ERR_FILE_TOO_LARGE_STREAM';
                            abortRequest(error);
                        }
                    });

                    stream.on('downloadProgress', progress => {
                        if (requestAborted) return;
                        if (progress.transferred > maxSizeBytes!) {
                             const error = new Error(`Download progress (${progress.transferred} bytes) exceeds limit of ${maxSizeBytes} bytes`);
                             (error as CustomError).code = 'ERR_FILE_TOO_LARGE_STREAM';
                             abortRequest(error);
                        }
                    });

                    stream.on('end', () => {
                        if (requestAborted) return;

                        log.debug(`[Size Check] Stream finished for ${options.url}. Total size: ${receivedBytes} bytes.`);
                        const body = Buffer.concat(dataChunks);
                        // Construct a response object similar to what got() would return
                        const finalResponse: Partial<CrawlerResponse> = {
                            body: body,
                            headers: responseHeaders || {},
                            statusCode: statusCode || 0,
                            requestUrl: options.url,
                        };
                        resolve(this._handler(null, options, finalResponse as CrawlerResponse));
                    });

                    stream.on('error', (error: any) => {
                        if (!requestAborted) {
                            requestAborted = true;
                            log.debug(`[Size Check] Stream error for ${options.url}: ${error.message}`);
                            resolve(this._handler(error, options));
                        }
                    });
                });
            } else {
                // Original behavior: full download via got()
                try {
                    const response = await got(gotOptions);
                    return this._handler(null, options, response);
                } catch (error) {
                    log.debug(error);
                    return this._handler(error, options);
                }
            }
        };

        // Handle preRequest (if defined by user)
        if (isFunction(options.preRequest)) {
             try {
                // Pass the original 'options' (not gotOptions) to user's preRequest
                options.preRequest!(options, async (err?: Error | null) => {
                    if (err) {
                        log.debug(`User preRequest aborted: ${err.message}`);
                        return this._handler(err, options);
                    }
                    // If user's preRequest is okay, execute the request function
                    return await requestFn();
                });
                // Return undefined because the actual execution is handled by the preRequest callback
                return undefined as any;

            } catch (err) {
                log.error(`Error in user preRequest execution: ${err}`);
                return this._handler(err, options); // Handle error from preRequest itself
            }
        } else {
            // No user preRequest, just execute the request function
            return await requestFn();
        }
    };

    private _handler = (error: unknown, options: RequestOptions, response?: CrawlerResponse): CrawlerResponse => {
        if (error) {
             // Handle new size check errors
            if ((error as CustomError).code === 'ERR_FILE_TOO_LARGE_HEAD' ||
                (error as CustomError).code === 'ERR_FILE_TOO_LARGE_RESPONSE_HEADER' ||
                (error as CustomError).code === 'ERR_FILE_TOO_LARGE_STREAM' ||
                (error as CustomError).code === 'ERR_MISSING_CONTENT_LENGTH_HEAD') {
                log.warn(`[Size Check] ${(error as Error).message} - ${options.url}`);
            }
            if (options.retries && options.retries > 0) {
                log.warn(`${error} occurred on ${options.url}. ${options.retries ? `(${options.retries} retries left)` : ""}`);
                setTimeout(() => {
                    options.retries!--;
                    this._execute(options as CrawlerOptions);
                }, options.retryInterval);
                return;
            } else {
                log.error(`${error} occurred on ${options.url}. Request failed.`);
                if (options.callback && typeof options.callback === "function") {
                    return options.callback(error, { options }, options.release);
                } else {
                    throw error;
                }
            }
        }
        if (!response.body) response.body = "";
        log.debug("Got " + (options.url || "html") + " (" + response.body.length + " bytes)...");
        response.options = options;

        response.charset = getCharset(response.headers);
        if (!response.charset) {
            const match = response.body.toString().match(/charset=['"]?([\w.-]+)/i);
            response.charset = match ? match[1].trim().toLowerCase() : null;
        }
        log.debug("Charset: " + response.charset);

        if (options.encoding !== null) {
            options.encoding = options.encoding ?? response.charset ?? "utf8";
            try {
                if (!Buffer.isBuffer(response.body)) response.body = Buffer.from(response.body);
                response.body = iconv.decode(response.body, options.encoding as string);
                response.body = response.body.toString();
            } catch (err) {
                log.error(err);
            }
        }

        if (options.isJson) {
            try {
                response.body = JSON.parse(response.body);
            } catch (_err) {
                log.warn("JSON parsing failed, body is not JSON. Set isJson to false to mute this warning.");
            }
        }

        if (options.jQuery === true && !options.isJson) {
            if (response.body === "" || !this._detectHtmlOnHeaders(response.headers)) {
                log.warn("response body is not HTML, skip injecting. Set jQuery to false to mute this warning.");
            } else {
                try {
                    response.$ = load(response.body);
                } catch (_err) {
                    log.warn("HTML detected failed. Set jQuery to false to mute this warning.");
                }
            }
        }

        if (options.callback && typeof options.callback === "function") {
            return options.callback(null, response, options.release);
        }
        return response;
    };

    public get queueSize(): number {
        return 0;
    }

    /**
     * @param rateLimiterId
     * @param property
     * @param value
     * @description Set the rate limiter property.
     * @version 2.0.0 Only support `rateLimit` change.
     * @example
     * ```js
     * const crawler = new Crawler();
     * crawler.setLimiter(0, "rateLimit", 1000);
     * ```
     */
    public setLimiter(rateLimiterId: number, property: string, value: unknown): void {
        if (!isNumber(rateLimiterId)) {
            log.error("rateLimiterId must be a number");
            return;
        }
        if (property === "rateLimit") {
            this._limiters.getRateLimiter(rateLimiterId).setRateLimit(value as number);
        }
        // @todo other properties
    }

    /**
     * @param options
     * @returns if there is a "callback" function in the options, return the result of the callback function. \
     * Otherwise, return a promise, which resolves when the request is successful and rejects when the request fails.
     * In the case of the promise, the resolved value will be the response object.
     * @description Send a request directly.
     * @example
     * ```js
     * const crawler = new Crawler();
     * crawler.send({
     *      url: "https://example.com",
     *      callback: (error, response, done) => { done(); }
     * });
     * await crawler.send("https://example.com");
     * ```
     */
    public send = async (options: RequestConfig): Promise<CrawlerResponse> => {
        options = getValidOptions(options);
        options.retries = options.retries ?? 0;
        setDefaults(options, this.options);
        options.skipEventRequest = isBoolean(options.skipEventRequest) ? options.skipEventRequest : true;
        delete options.preRequest;
        return await this._execute(options);
    };
    /**
     * @deprecated
     * @description Old interface version. It is recommended to use `Crawler.send()` instead.
     * @see Crawler.send
     */
    public direct = async (options: RequestConfig): Promise<CrawlerResponse> => {
        return await this.send(options);
    };

    /**
     * @param options
     * @description Add a request to the queue.
     * @example
     * ```js
     * const crawler = new Crawler();
     * crawler.add({
     *     url: "https://example.com",
     *     callback: (error, response, done) => { done(); }
     * });
     * ```
     */
    public add = (options: RequestConfig): void => {
        let optionsArray = Array.isArray(options) ? options : [options];
        optionsArray = flattenDeep(optionsArray);
        optionsArray.forEach(options => {
            try {
                options = getValidOptions(options) as RequestOptions;
            } catch (err) {
                log.warn(err);
                return;
            }
            setDefaults(options, this.options);
            options.headers = { ...this.options.headers, ...options.headers };
            if (!this.options.skipDuplicates) {
                this._schedule(options as CrawlerOptions);
                return;
            }

            this.seen
                .exists(options, options.seenreq)
                .then((rst: any) => {
                    if (!rst) {
                        this._schedule(options as CrawlerOptions);
                    }
                })
                .catch((error: unknown) => log.error(error));
        });
    };
    /**
     * @deprecated
     * @description Old interface version. It is recommended to use `Crawler.add()` instead.
     * @see Crawler.add
     */
    public queue = (options: RequestConfig): void => {
        return this.add(options);
    };
}

export default Crawler;