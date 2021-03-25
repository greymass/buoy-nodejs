import config from 'config'
import {logger as baseLogger} from '../common'
import {MemoryBroker} from './memory-broker'
import {Broker} from './broker'

const logger = baseLogger.child({module: 'broker'})

let broker: Broker
const brokerConf = config.get('broker') as any

switch (brokerConf.type) {
    case 'memory':
        logger.info({config: brokerConf}, 'using memory broker')
        broker = new MemoryBroker(brokerConf, logger)
        break
    default:
        throw new Error(`Unknown broker type ${brokerConf.type}`)
}

export default broker

export type {Broker, DeliveryState, SendOptions, Unsubscriber, Updater} from './broker'
