import * as amqp from 'amqplib/callback_api'



export class Publish{
    host: string
    port: string
    exchange: string
    errorMessage = ''

    connection: amqp.Connection | undefined
    channel: amqp.Channel | undefined


    constructor(exchange: string, host: string, port:string){
        this.host = host
        this.port = port
        this.exchange = exchange
    }

    init():Promise<boolean>{
        return new Promise((resolve, reject) => {
            amqp.connect(`amqp://${this.host}:${this.port}`, (error0, connection)=>{
                if(error0) reject(error0)
                this.connection = connection
                connection.createChannel((error1, channel) => {
                    if(error1) reject(error1)
                    channel.assertExchange(this.exchange, "direct", { durable: false });
                    this.channel = channel
                    resolve(true)
                })
            })
        })
    }



    send<T>( message: T, queue:string):boolean{
            if(!this.connection || !this.channel) {
                this.errorMessage = "AMQP chanel not init"
                return false
            }
            let msg = typeof message === 'string' ? message : JSON.stringify(message) 
            return this.channel.publish(this.exchange, queue, Buffer.from(msg));
    }


    close(){
        return new Promise((resolve, reject) => {
            
            if(this.channel) this.channel.close(reject)
            if(this.connection) this.connection.close()
            this.channel = undefined
            this.connection = undefined
            resolve(true)
        })
    }
}

