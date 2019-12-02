import config from 'config'
import * as http from 'http'
import LRU from 'lru-cache'
import {URL} from 'url'
import * as WebSocket from 'ws'

import {logger} from './common'
import version from './version'

// simple http post > websocket forwarder
// handles only one connection per UUID at a time

const httpServer = http.createServer(handleRequest)
const websocketServer = new WebSocket.Server({server: httpServer})
const connections = new Map<string, WebSocket>()
const waiting = new Map<string, Function>()
const cache = new LRU<string, Buffer>({maxAge: 60 * 60 * 1000})

function getUUID(request: http.IncomingMessage) {
    const url = new URL(request.url || '', 'http://localhost')
    const uuid = url.pathname.slice(1)
    if (uuid.length < 10) {
        throw new HttpError('Invalid UUID', 400)
    }
    return uuid
}

async function handleConnection(socket: WebSocket, request: http.IncomingMessage) {
    const uuid = getUUID(request)
    const log = logger.child({uuid})
    if (connections.has(uuid)) {
        log.warn('replacing existing socket connection')
        connections.get(uuid)!.close(4001, 'Replaced by other connection')
    } else {
        log.info('socket connected')
    }
    connections.set(uuid, socket)
    socket.once('close', (code, reason) => {
        log.info({code, reason}, 'socket closed')
        let existing = connections.get(uuid)
        if (existing === socket) {
            connections.delete(uuid)
        } else {
            log.info('banani')
        }
    })
    if (waiting.has(uuid)) {
        waiting.get(uuid)!(socket)
    } else if (cache.has(uuid)) {
        log.info('forwarding buffered message')
        socket.send(cache.get(uuid)!)
        cache.del(uuid)
    }
}

class HttpError extends Error {
    statusCode: number
    constructor(message: string, statusCode: number) {
        super(message)
        this.statusCode = statusCode
    }
}

async function handlePost(request: http.IncomingMessage, response: http.ServerResponse) {
    const uuid = getUUID(request)
    const log = logger.child({uuid})
    const data = await readBody(request)
    if (data.byteLength === 0) {
        throw new HttpError('Unable to forward empty body', 400)
    }
    const existingSocket = connections.get(uuid)
    if (existingSocket) {
        // recipient already connected, just forward data
        existingSocket.send(data)
        response.setHeader('x-buoy-delivery', 'delivered')
        log.info('data delivered')
    } else if (request.headers['x-buoy-wait']) {
        // wait for delivery
        const deliveryTimeout = Math.min(Number(request.headers['x-buoy-wait']), 60 * 60) * 1000
        if (!Number.isFinite(deliveryTimeout)) {
            throw new HttpError('Invalid x-buoy-wait timeout', 400)
        }
        log.info({timeout: deliveryTimeout}, 'waiting for client to connect')
        await new Promise((resolve, reject) => {
            waiting.set(uuid, (socket: WebSocket) => {
                waiting.delete(uuid)
                socket.send(data)
                resolve()
            })
            setTimeout(() => {
                waiting.delete(uuid)
                reject(new HttpError('Timed out waiting for connection', 408))
            }, deliveryTimeout)
        })
        response.setHeader('x-buoy-delivery', 'delivered')
        log.info('data delivered')
    } else {
        // else we buffer the data and replay it when the recipient connects
        cache.set(uuid, data)
        response.setHeader('x-buoy-delivery', 'buffered')
    }
}

function readBody(request: http.IncomingMessage) {
    return new Promise<Buffer>((resolve, reject) => {
        const chunks: Buffer[] = []
        request.on('error', reject)
        request.on('data', (chunk) => {
            chunks.push(chunk)
        })
        request.on('end', () => {
            resolve(Buffer.concat(chunks))
        })
    })
}

websocketServer.on('connection', (socket, request) => {
    logger.debug('new connection')
    handleConnection(socket as any, request).catch((error) => {
        logger.warn(error, 'error handling websocket connection')
        socket.close()
    })
})

function handleRequest(request: http.IncomingMessage, response: http.ServerResponse) {
    if (request.method !== 'POST') {
        response.statusCode = 405
        response.write('Method not allowed')
        response.end()
        return
    }
    handlePost(request, response)
        .then(() => {
            response.statusCode = 200
            response.write('Ok')
            response.end()
        })
        .catch((error) => {
            if (error instanceof HttpError) {
                logger.info(error, 'error handling post request')
                response.statusCode = error.statusCode
                response.write(error.message)
            } else {
                logger.warn(error, 'unexpected error handling post request')
                response.statusCode = 500
                response.write('Internal server error')
            }
            response.end()
        })
}

export async function main() {
    const port = Number.parseInt(config.get('port'))
    if (!Number.isFinite(port)) {
        throw new Error('Invalid port number')
    }
    logger.info({version}, 'starting')
    await new Promise((resolve, reject) => {
        httpServer.listen(port, resolve)
        httpServer.once('error', reject)
    })
    logger.info({port}, 'server running')
}

function ensureExit(code: number) {
    process.exitCode = code
    process.nextTick(() => {
        process.exit(code)
    })
}

if (module === require.main) {
    process.once('uncaughtException', (error) => {
        logger.error(error, 'Uncaught exception')
        ensureExit(1)
    })
    main().catch((error) => {
        logger.fatal(error, 'Unable to start application')
        ensureExit(1)
    })
}
