import * as config from 'config'
import * as http from 'http'
import { URL } from 'url'
import * as WebSocket from 'ws'
import {logger} from './common'
import version from './version'

// simple http post > websocket forwarder
// handles only one connection at a time
// does not buffer response

const httpServer = http.createServer(handleRequest)
const websocketServer = new WebSocket.Server({server: httpServer})
const connections = new Map<string, WebSocket>()

function getUUID(request: http.IncomingMessage) {
    const url = new URL(request.url || '', 'http://localhost')
    const uuid = url.pathname
    if (uuid.length < 10) {
        throw new Error('Invalid UUID')
    }
    return uuid
}

async function handleConnection(socket: WebSocket, request: http.IncomingMessage) {
    const uuid = getUUID(request)
    const log = logger.child({uuid})
    connections.set(uuid, socket)
    socket.once('close', (code, reason) => {
        log.info({code, reason}, 'socket closed')
        connections.delete(uuid)
    })
    log.info('socket connected')
}

async function handlePost(request: http.IncomingMessage, response: http.ServerResponse) {
    const uuid = getUUID(request)
    const log = logger.child({uuid})
    const socket = connections.get(uuid)
    if (!socket) {
        throw new Error('No connection with matching UUID')
    }
    const data = await readBody(request)
    socket.send(data)
    log.info('data fowarded')
}

function readBody(request: http.IncomingMessage) {
    return new Promise<Buffer>((resolve, reject) => {
        const chunks: Buffer[] = []
        request.on('error', reject)
        request.on('data', (chunk) => { chunks.push(chunk) })
        request.on('end', () => {
            resolve(Buffer.concat(chunks))
        })
    })
}

websocketServer.on('connection', (socket, request) => {
    logger.debug('new connection')
    handleConnection(socket, request)
        .catch((error) => {
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
            logger.warn(error, 'error handling post request')
            response.statusCode = 400
            response.write('Bad request')
            response.end()
        })
}

export async function main() {
    const port = parseInt(config.get('port'), 10)
    if (!isFinite(port)) {
        throw new Error('Invalid port number')
    }
    logger.info({version}, 'starting')
    await new Promise((resolve, reject) => {
        httpServer.listen(port, resolve)
        httpServer.once('error', reject)
    })
    logger.info({port}, 'server running')
}

function ensureExit(code: number, timeout = 3000) {
    process.exitCode = code
    setTimeout(() => { process.exit(code) }, timeout)
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
