import * as amqp from 'amqplib/callback_api'
import { Socket } from 'socket.io'
import { Connection } from './Connection2'
import { EventEmitter } from 'stream'



/**
 * доступные события: 
 * - 'ready'
 * - 'error'
 * - 'message'
 * - 'close'
 */
export class Consume extends EventEmitter{
    connection: amqp.Connection | undefined
    channel: amqp.Channel | undefined
    queue: amqp.Replies.AssertQueue | undefined

    exchangeName = ''
    queueName = ''

    host: string
    port: string


    constructor(exchange: string, queue: string, host: string, port: string ){
        super()
        this.exchangeName = exchange
        this.queueName = queue
        this.host = host
        this.port = port 
        this.onMessage = this.onMessage.bind(this)
    }


    connect(): Promise<Consume> {
        return new Promise((resolve, reject) => {
            try{
                amqp.connect(`amqp://${this.host}:${this.port}`, (error0, connection) => {
                    if (error0) throw error0;
                    this.connection = connection

                    connection.createChannel((error1, channel) => {
                        if (error1)  throw error1;
                        this.channel = channel

                        channel.prefetch(1)

                        channel.assertQueue(this.queueName, {  }, (error3, queue) => {
                            if(error3) throw error3
                            channel.bindQueue(queue.queue, this.exchangeName, this.queueName)
                            this.queue = queue
                            channel.consume(queue.queue, this.onMessage)
                            this.emit('ready')
                            resolve(this)
                        })
                    }, );
                });
            } catch(err) {
                this.emit('error', err)
                reject(err)
            }
        })
    }


    onMessage(msg:amqp.Message | null){
        if(!msg) return 
        const message = msg.content.toString()
        console.log('rmq -> ', message);
        this.emit('message', message)
        this.channel?.ack(msg)
    }


    close(){
        return new Promise((resolve, reject) => {
            if(this.channel) {
                this.channel.close(reject)
                this.channel = undefined
            }
            if(this.connection){
                this.connection.close()
                this.connection = undefined
            }
            this.emit('close')
            resolve(true)
        })
        
    }


}