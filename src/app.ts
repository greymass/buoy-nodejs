import config from 'config'
import * as http from 'http'
import LRU from 'lru-cache'
import {URL} from 'url'
import * as WebSocket from 'ws'

import {logger} from './common'
import version from './version'

// simple http post > websocket forwarder
// handles only one connection per UUID at a time

const HEARTBEAT_INTERVAL = 10 * 1000

const httpServer = http.createServer(handleRequest)
const websocketServer = new WebSocket.Server({server: httpServer})
const connections = new Map<string, Connection>()
const waiting = new Map<string, Function>()
const cache = new LRU<string, Buffer>({
    maxAge: 60 * 60 * 1000, // 1 hour
    max: 5e8, // 500 mb
    length: (item) => item.byteLength,
})

class Connection {
    static seq = 0

    alive: boolean
    closed: boolean
    id: number

    private cleanupCallback: () => void
    private socket: WebSocket
    private timer: NodeJS.Timeout
    private log: typeof logger

    constructor(socket: WebSocket, cleanup: () => void) {
        this.id = ++Connection.seq
        this.log = logger.child({conn: this.id})
        this.socket = socket
        this.closed = false
        this.alive = true
        this.cleanupCallback = cleanup
        this.socket.on('close', () => {
            this.didClose()
        })
        this.socket.on('pong', () => {
            this.alive = true
        })
        this.timer = setInterval(() => {
            if (this.alive) {
                this.alive = false
                this.socket.ping()
            } else {
                this.destroy()
            }
        }, HEARTBEAT_INTERVAL)
    }

    private didClose() {
        this.log.debug({alive: this.alive, closed: this.closed}, 'did close')
        this.alive = false
        clearTimeout(this.timer)
        if (this.closed === false) {
            this.cleanupCallback()
        }
        this.closed = true
    }

    send(data: Buffer) {
        this.log.debug({size: data.byteLength}, 'send data')
        this.socket.send(data)
    }

    close(code?: number, reason?: string) {
        this.socket.close(code, reason)
        this.didClose()
    }

    destroy() {
        this.socket.terminate()
        this.didClose()
    }
}

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
    const connection = new Connection(socket, () => {
        log.info('connection closed')
        let existing = connections.get(uuid)
        if (existing && existing.id === connection.id) {
            connections.delete(uuid)
        }
    })
    const log = logger.child({uuid, conn: connection.id})
    if (connections.has(uuid)) {
        log.info('replacing existing connection')
        connections.get(uuid)!.close(4001, 'Replaced by other connection')
    } else {
        log.info('new connection')
    }
    connections.set(uuid, connection)
    if (waiting.has(uuid)) {
        log.debug('resolving waiting connection')
        waiting.get(uuid)!(connection)
    } else if (cache.has(uuid)) {
        log.debug('forwarding buffered message')
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
        throw new HttpError('Unable to forward empty message', 400)
    }
    let connection = connections.get(uuid)
    if (!connection && request.headers['x-buoy-wait']) {
        const deliveryTimeout = Math.min(Number(request.headers['x-buoy-wait']), 60 * 60) * 1000
        if (!Number.isFinite(deliveryTimeout)) {
            throw new HttpError('Invalid X-Buoy-Wait timeout', 400)
        }
        log.info({timeout: deliveryTimeout}, 'waiting for client to connect')
        connection = await new Promise<Connection>((resolve, reject) => {
            waiting.set(uuid, (result: Connection) => {
                waiting.delete(uuid)
                resolve(result)
            })
            setTimeout(() => {
                waiting.delete(uuid)
                reject(new HttpError('Timed out waiting for connection', 408))
            }, deliveryTimeout)
        })
    }
    if (connection) {
        connection.send(data)
        response.setHeader('X-Buoy-Delivery', 'delivered')
        log.info({conn: connection.id}, 'data delivered')
    } else {
        log.info('buffering data')
        cache.set(uuid, data)
        response.setHeader('X-Buoy-Delivery', 'buffered')
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
    handleConnection(socket as any, request).catch((error) => {
        logger.error(error, 'error handling websocket connection')
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
                logger.error(error, 'unexpected error handling post request')
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
