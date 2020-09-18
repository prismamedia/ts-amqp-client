import { EventEmitter } from 'events';

export type TEventMap = Record<string, any>;

type TInternalEventMap<TMap extends TEventMap> = { error: Error } & TMap;

export type TEventName<TMap extends TEventMap> = Extract<
  keyof TInternalEventMap<TMap>,
  string
>;

export type TEventData<
  TMap extends TEventMap,
  TName extends TEventName<TMap> = TEventName<TMap>
> = TInternalEventMap<TMap>[TName];

export class TypedEventEmitter<TMap extends TEventMap> extends EventEmitter {
  public emit<TName extends TEventName<TMap>>(
    ...[name, data]: TEventData<TMap, TName> extends undefined
      ? [name: TName]
      : [name: TName, data: TEventData<TMap, TName>]
  ) {
    return super.emit(name, data);
  }

  public on<TName extends TEventName<TMap>>(
    name: TName,
    listener: (data: TEventData<TMap, TName>) => void,
  ) {
    return super.on(name, listener);
  }
}
