import config from 'config'
import cluster from 'cluster'

import baseLogger from '../logger'
import {Broker} from './broker'

import {MemoryBroker} from './memory-broker'
import {MqttBroker} from './mqtt-broker'

async function setupBroker() {
    const logger = baseLogger.child({module: 'broker'})
    const brokerConf = config.get('broker') as any
    logger.debug('using %s broker', brokerConf.type)

    let broker: Broker

    switch (brokerConf.type) {
        case 'memory':
            if (!cluster.isPrimary) {
                throw new Error('Memory broker cannot be used with more than one worker')
            }
            broker = new MemoryBroker(brokerConf, logger)
            break
        case 'mqtt':
            broker = new MqttBroker(brokerConf, logger)
            break
        default:
            throw new Error(`Unknown broker type ${brokerConf.type}`)
    }

    await broker.init()

    return broker
}

export default setupBroker
export * from './broker'
