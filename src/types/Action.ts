export interface Action<T> {
    id: string;
    uid: string;
    action: string;
    data: T;
    datetime: Date;
    entity: string;
    synced: 0 | 1;
    user_id: string;
}
