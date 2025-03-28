import test from "ava";
import { testCb } from "./lib/avaTestCb.js";
import nock from "nock";
import Crawler from "../dist/index.js";
import { Buffer } from 'buffer';

const BASE_URL = "http://test-maxsize.com";
const SMALL_FILE_SIZE = 5 * 1024 * 1024; // 5 MB
const LARGE_FILE_SIZE = 15 * 1024 * 1024; // 15 MB
const LIMIT = 10 * 1024 * 1024; // 10 MB Limit

// Helper to create a buffer of a specific size
const createBuffer = (size, fill = 'a') => Buffer.alloc(size, fill);

const smallBuffer = createBuffer(SMALL_FILE_SIZE);
const largeBuffer = createBuffer(LARGE_FILE_SIZE);

test.beforeEach(t => {
    nock.cleanAll();
    t.context.crawler = new Crawler({
        // silence: true,
        jQuery: false,
        retryInterval: 0,
        retries: 0,
    });
});

test.afterEach(t => {
    nock.cleanAll();
    t.context.crawler = null;
});

// --- Test Cases ---

testCb(test, "[maxSizeBytes] Should allow download if no limit is set", async t => {
    nock(BASE_URL)
        .get("/large-file")
        .reply(200, largeBuffer, { 'Content-Length': LARGE_FILE_SIZE });

    t.context.crawler.add({
        url: `${BASE_URL}/large-file`,
        callback: (error, res, done) => {
            t.is(error, null, "Should not have an error");
            t.is(res.statusCode, 200);
            t.is(res.body.length, LARGE_FILE_SIZE, "Body length should match large file size");
            done();
            t.end();
        },
    });
});

testCb(test, "[maxSizeBytes] HEAD check: Should reject if Content-Length exceeds limit", async t => {
    nock(BASE_URL)
        .head("/large-file")
        .reply(200, '', { 'Content-Length': LARGE_FILE_SIZE });

    t.context.crawler.add({
        url: `${BASE_URL}/large-file`,
        maxSizeBytes: LIMIT,
        callback: (error, res, done) => {
            t.truthy(error, "Should have an error");
            t.is(error.code, 'ERR_FILE_TOO_LARGE_HEAD', "Error code should indicate HEAD check failure");
            t.regex(error.message, /exceeds limit/);
            done();
            t.end();
        },
    });
});

testCb(test, "[maxSizeBytes] HEAD check: Should allow if Content-Length is within limit", async t => {
    nock(BASE_URL)
        .head("/small-file")
        .reply(200, '', { 'Content-Length': SMALL_FILE_SIZE });
    nock(BASE_URL)
        .get("/small-file")
        .reply(200, smallBuffer, { 'Content-Length': SMALL_FILE_SIZE });

    t.context.crawler.add({
        url: `${BASE_URL}/small-file`,
        maxSizeBytes: LIMIT,
        callback: (error, res, done) => {
            t.is(error, null, "Should not have an error");
            t.is(res.statusCode, 200);
            t.is(res.body.length, SMALL_FILE_SIZE, "Body length should match small file size");
            done();
            t.end();
        },
    });
});

testCb(test, "[maxSizeBytes] HEAD check: Should reject if Content-Length missing and rejectOnMissingContentLength is true", async t => {
    nock(BASE_URL)
        .head("/missing-cl")
        .reply(200, '', { /* No Content-Length */ });

    t.context.crawler.add({
        url: `${BASE_URL}/missing-cl`,
        maxSizeBytes: LIMIT,
        rejectOnMissingContentLength: true,
        callback: (error, res, done) => {
            t.truthy(error, "Should have an error");
            t.is(error.code, 'ERR_MISSING_CONTENT_LENGTH_HEAD', "Error code should indicate missing Content-Length rejection");
            done();
            t.end();
        },
    });
});

testCb(test, "[maxSizeBytes] HEAD check: Should proceed if Content-Length missing and rejectOnMissingContentLength is false (and GET is small)", async t => {
    nock(BASE_URL)
        .head("/missing-cl-small")
        .reply(200, '', { /* No Content-Length */ });
    nock(BASE_URL)
        .get("/missing-cl-small")
        .reply(200, smallBuffer, { /* No Content-Length */ });

    t.context.crawler.add({
        url: `${BASE_URL}/missing-cl-small`,
        maxSizeBytes: LIMIT,
        rejectOnMissingContentLength: false,
        callback: (error, res, done) => {
            t.is(error, null, "Should not have an error");
            t.is(res.statusCode, 200);
            t.is(res.body.length, SMALL_FILE_SIZE, "Body length should match small file size");
            done();
            t.end();
        },
    });
});

testCb(test, "[maxSizeBytes] HEAD check: Should proceed if HEAD fails (405 Method Not Allowed) and GET is small", async t => {
    nock(BASE_URL)
        .head("/head-not-allowed")
        .reply(405);
    nock(BASE_URL)
        .get("/head-not-allowed")
        .reply(200, smallBuffer, { 'Content-Length': SMALL_FILE_SIZE });

    t.context.crawler.add({
        url: `${BASE_URL}/head-not-allowed`,
        maxSizeBytes: LIMIT,
        callback: (error, res, done) => {
            t.is(error, null, "Should not have an error despite HEAD failure");
            t.is(res.statusCode, 200);
            t.is(res.body.length, SMALL_FILE_SIZE);
            done();
            t.end();
        },
    });
});

// --- Stream Monitoring Tests ---

testCb(test, "[maxSizeBytes] Stream check: Should reject if GET Content-Length header exceeds limit (HEAD was ok)", async t => {
    nock(BASE_URL)
        .head("/cl-mismatch-large")
        .reply(200, '', { 'Content-Length': SMALL_FILE_SIZE });
    nock(BASE_URL)
        .get("/cl-mismatch-large")
        .reply(200, largeBuffer, { 'Content-Length': LARGE_FILE_SIZE });

    t.context.crawler.add({
        url: `${BASE_URL}/cl-mismatch-large`,
        maxSizeBytes: LIMIT,
        callback: (error, res, done) => {
            t.truthy(error, "Should have an error");
            t.is(error.code, 'ERR_FILE_TOO_LARGE_RESPONSE_HEADER', "Error code should indicate GET header check failure");
            t.regex(error.message, /exceeds limit/);
            done();
            t.end();
        },
    });
});

testCb(test, "[maxSizeBytes] Stream check: Should reject during download if data exceeds limit (no valid Content-Length)", async t => {
    nock(BASE_URL)
        .head("/stream-large-no-cl")
        .reply(200, '', { /* No Content-Length */ });
    nock(BASE_URL)
        .get("/stream-large-no-cl")
        .reply(200, largeBuffer, { /* No Content-Length */ });

    t.context.crawler.add({
        url: `${BASE_URL}/stream-large-no-cl`,
        maxSizeBytes: LIMIT,
        rejectOnMissingContentLength: false,
        callback: (error, res, done) => {
            t.truthy(error, "Should have an error");
            t.is(error.code, 'ERR_FILE_TOO_LARGE_STREAM', "Error code should indicate stream monitoring failure");
            t.regex(error.message, /exceeds limit/);
            t.true(nock.isDone(), "Nock should have processed the GET request start");
            done();
            t.end();
        },
    });
});

testCb(test, "[maxSizeBytes] Stream check: Should allow download if stream size is within limit (no valid Content-Length)", async t => {
    nock(BASE_URL)
        .head("/stream-small-no-cl")
        .reply(200, '', { /* No Content-Length */ });
    nock(BASE_URL)
        .get("/stream-small-no-cl")
        .reply(200, smallBuffer, { /* No Content-Length */ });

    t.context.crawler.add({
        url: `${BASE_URL}/stream-small-no-cl`,
        maxSizeBytes: LIMIT,
        rejectOnMissingContentLength: false,
        callback: (error, res, done) => {
            t.is(error, null, "Should not have an error");
            t.is(res.statusCode, 200);
            t.is(res.body.length, SMALL_FILE_SIZE, "Body length should match small file size");
            done();
            t.end();
        },
    });
});

// --- Integration with preRequest ---

testCb(test, "[maxSizeBytes] Should work correctly with user preRequest (HEAD reject)", async t => {
    let preRequestCalled = false;
    nock(BASE_URL)
        .head("/large-prereq")
        .reply(200, '', { 'Content-Length': LARGE_FILE_SIZE });

    t.context.crawler.add({
        url: `${BASE_URL}/large-prereq`,
        maxSizeBytes: LIMIT,
        preRequest: (options, done) => {
            preRequestCalled = true;
            options.headers['X-User-Prefetch'] = 'true';
            done();
        },
        callback: (error, res, done) => {
            t.true(preRequestCalled, "User preRequest should have been called");
            t.truthy(error, "Should have an error from HEAD check");
            t.is(error.code, 'ERR_FILE_TOO_LARGE_HEAD');
            done();
            t.end();
        },
    });
});

testCb(test, "[maxSizeBytes] Should work correctly with user preRequest (Stream reject)", async t => {
    let preRequestCalled = false;
    nock(BASE_URL)
        .head("/stream-large-prereq")
        .reply(200, '', { /* No Content-Length */ });
     nock(BASE_URL)
        .get("/stream-large-prereq")
        .reply(200, largeBuffer, { /* No Content-Length */ });

    t.context.crawler.add({
        url: `${BASE_URL}/stream-large-prereq`,
        maxSizeBytes: LIMIT,
        rejectOnMissingContentLength: false,
        preRequest: (options, done) => {
            preRequestCalled = true;
            options.headers['X-User-Prefetch'] = 'true';
            done();
        },
        callback: (error, res, done) => {
            t.true(preRequestCalled, "User preRequest should have been called");
            t.truthy(error, "Should have an error from stream check");
            t.is(error.code, 'ERR_FILE_TOO_LARGE_STREAM');
            t.true(nock.isDone());
            done();
            t.end();
        },
    });
});

testCb(test, "[maxSizeBytes] Should abort if user preRequest calls done(error)", async t => {
    let preRequestCalled = false;
    nock(BASE_URL).head("/prereq-abort").reply(200);
    nock(BASE_URL).get("/prereq-abort").reply(200);

    t.context.crawler.add({
        url: `${BASE_URL}/prereq-abort`,
        maxSizeBytes: LIMIT,
        preRequest: (options, done) => {
            preRequestCalled = true;
            const userError = new Error("User preRequest decided to abort");
            userError.code = 'USER_ABORT';
            done(userError);
        },
        callback: (error, res, done) => {
            t.true(preRequestCalled, "User preRequest should have been called");
            t.truthy(error, "Should have the error from preRequest");
            t.is(error.code, 'USER_ABORT');
            done();
            t.end();
        },
    });
});