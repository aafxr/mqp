import { Action } from "./Action"

export type MessageType = {
    message: {
        from: string,
        to: string,
        text: string,
        date: Date,
        primary_entity_id: string
    }
} | {
    action: Action<any>
} | {
    receive: string
} | {
    join: {travelID: string}
} | {
    leave: {travelID: string}
}