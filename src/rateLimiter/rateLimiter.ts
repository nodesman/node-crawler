import { multiPriorityQueue } from "../lib/index.js";
import Cluster from "./cluster.js";

export interface RateLimiterOptions {
    maxConcurrency: number;
    rateLimit: number;
    priorityCount: number;
    defaultPriority: number;
    cluster?: Cluster;
}

class RateLimiter {
    private _Id?: string;
    private _maxConcurrency: number;
    private _waitingTasks: multiPriorityQueue<(done: () => void, limiter: null) => void>;
    private _priorityCount: number;
    private _defaultPriority: number;
    private _cluster?: Cluster;

    public nextRequestTime: number;
    public rateLimit: number;
    public runningTasksNumber: number;

    constructor({ maxConcurrency, rateLimit, priorityCount = 1, defaultPriority = 0, cluster }: RateLimiterOptions) {
        if (!Number.isInteger(maxConcurrency) || !Number.isInteger(rateLimit) || !Number.isInteger(priorityCount)) {
            throw new Error("maxConcurrency, rateLimit and priorityCount must be positive integers");
        }
        this._maxConcurrency = maxConcurrency;
        this._priorityCount = priorityCount;
        this._defaultPriority = Number.isInteger(defaultPriority)
            ? defaultPriority
            : Math.floor(this._priorityCount / 2);
        this._defaultPriority >= priorityCount ? priorityCount - 1 : defaultPriority;
        this._waitingTasks = new multiPriorityQueue<(done: () => void, limiter: null) => void>(priorityCount);
        this.nextRequestTime = Date.now();
        this._cluster = cluster;

        this.rateLimit = rateLimit;
        this.runningTasksNumber = 0;
    }
    size(): number {
        return this._waitingTasks.size();
    }

    setId(id: string) {
        this._Id = id;
    }
    setRateLimit(rateLimit: number): void {
        if (!Number.isInteger(rateLimit)) {
            throw new Error("rateLimit must be positive integers");
        }
        this.rateLimit = rateLimit;
    }

    submit(options: number | { priority: number }, task: (done: () => void, limiter: null) => void): void {
        const priority = typeof options === "number" ? options : options.priority;
        const validPriority = Number.isInteger(priority) ? priority : this._defaultPriority;
        const clampedPriority = validPriority > this._priorityCount - 1 ? this._priorityCount - 1 : validPriority;
        this._waitingTasks.enqueue(task, clampedPriority);
        this._tryToRun();
    }

    private _tryToRun(): void {
        if (this.runningTasksNumber < this._maxConcurrency && this.hasWaitingClients()) {
            ++this.runningTasksNumber;
            const wait = Math.max(this.nextRequestTime - Date.now(), 0);
            this.nextRequestTime = Date.now() + wait + this.rateLimit;
            const task = this.dequeue();
            const next = task.next;
            setTimeout(() => {
                const done = () => {
                    --this.runningTasksNumber;
                    this._tryToRun();
                };
                next(done, null);
            }, wait);
        }
    }

    hasWaitingClients(): boolean {
        if (this.size()) {
            return true;
        }
        if (this._cluster && this._cluster._waitingTasks()) {
            return true;
        }
        return false;
    }

    dequeue(): { next?: (done: () => void, limiter: null) => void; limiter: null } | undefined {
        if (this.size()) {
            return {
                next: this._waitingTasks.dequeue(),
                limiter: null,
            };
        }
        return this._cluster?.dequeue();
    }
}

export default RateLimiter;
