import "dotenv/config";

import {MessageType} from "./types/MessageType";
import {Server, Socket} from "socket.io";
import {Publish, Consume, Group} from "./class";

import {createServer} from 'https'
import * as fs from "fs";
import {delay} from "./utils/delay";

const socket_hostname = process.env.SOCKET_HOST as string;
const rabitmq_hostname = process.env.RABITMQ_HOST as string;
const socket_port = process.env.SOCKET_PORT as string;
const rabbit_port = process.env.RABBIT_PORT as string;


async function socket_init() {
    // const httpServer = createServer(
    //     {
    //         key: fs.readFileSync("/var/socket/build/key.pem"),
    //         cert: fs.readFileSync("/var/socket/build/cert.pem"),
    //         passphrase: 'socket123456'
    //     }
    // )
    // const io = new Server(httpServer);
    const io = new Server(Number(socket_port), {
        cors: {
            origin: `*`,
            methods: ["GET", "POST"]
        },
    });


    const userGroups = new Group<Socket>()

    const travelPublisher = new Publish('travel', rabitmq_hostname, rabbit_port)

    let travelPublisherReady = await travelPublisher.connect().catch(console.log)
    travelPublisher.on(Publish.CONNECTED_EVENT, () => travelPublisherReady = true)
    travelPublisher.on(Publish.CONNECTION_ERROR_EVENT, () => console.log('publisher connection error, try reconnect'))


    async function createNewQueue(queueName: string, userID?: string, socket?: Socket) {
        const c = new Consume('travel', queueName, rabitmq_hostname, rabbit_port)

        c.on(Consume.MESSAGE_EVENT, function (msg) {
            console.log(c.queueName, ' -> ', msg)
            const g = userGroups.getItems(c.queueName)
            if (g) g.forEach(s => s.send(msg))
        })

        c.on(Consume.CLOSE_EVENT, () => userGroups.clearGroup(c.queueName))
        c.on(Consume.CONNECTION_ERROR_EVENT, () => console.log('consume connection error, try reconnect') )

        if (socket) userGroups.add(queueName, socket)
        await c.connect().catch(console.log)
        return c
    }


    io.on("connection", (socket) => {

        socket.on("join", async (msg: MessageType) => {
            try {
                if ('join' in msg) {
                    const travelID = msg.join?.travelID
                    if (!travelID) return
                    if (!userGroups.hasGroup(travelID)) await createNewQueue(travelID, socket.id, socket)
                    else {
                        userGroups.add(travelID, socket)
                    }
                }
            } catch (error) {
                console.log((<Error>error).message)
            }

        })


        socket.on("leave", (msg: MessageType) => {
            try {
                if ('leave' in msg) {
                    const travelID = msg.leave?.travelID
                    if (!travelID) return
                    if (userGroups.isInGroup(travelID, socket)) userGroups.delete(travelID, socket)
                }
            } catch (error) {
                console.log((<Error>error).message)
            }
        })


        socket.on("message", async (msg: MessageType) => {
            try {
                console.log('message -> ', msg)
                msg = JSON.parse(msg as unknown as string)
                if (!travelPublisherReady) {
                    console.log('Publisher not ready');
                    return
                }

                if ('message' in msg) {
                    const travelID = msg.message?.primary_entity_id
                    if (travelID) {
                        console.log(travelPublisher.send(msg, travelID), ' ', msg)
                    }
                    return
                }
            } catch (error) {
                console.log((<Error>error).message)
            }
        });

        socket.on("disconnect", () => {
            userGroups.deleteFromAllGroups(socket)
        });
    });

    // httpServer.listen(Number(socket_port))
}


socket_init().catch(console.error)

