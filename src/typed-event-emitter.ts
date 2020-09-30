import { errorMonitor, EventEmitter } from 'events';

export type TEventMap = Record<string, any>;

type TInternalEventMap<TMap extends TEventMap> = {
  error: Error;
  [errorMonitor]: Error;
} & TMap;

export type TEventName<TMap extends TEventMap> = Extract<
  keyof TInternalEventMap<TMap>,
  string | Symbol
>;

export type TEventData<
  TMap extends TEventMap,
  TName extends TEventName<TMap> = TEventName<TMap>
> = TInternalEventMap<TMap>[TName];

export type TEventListener<
  TMap extends TEventMap,
  TName extends TEventName<TMap> = TEventName<TMap>
> = (data: TEventData<TMap, TName>) => void;

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
    listener: TEventListener<TMap, TName>,
  ) {
    return super.on(name, listener);
  }

  public once<TName extends TEventName<TMap>>(
    name: TName,
    listener: TEventListener<TMap, TName>,
  ) {
    return super.once(name, listener);
  }

  public listenerCount(name: TEventName<TMap>) {
    return super.listenerCount(name);
  }

  public async wait<TName extends TEventName<TMap>>(
    ...names: TName[]
  ): Promise<TEventData<TMap, TName>> {
    return new Promise((resolve) => {
      const listeners = [...new Set(names)].map((name): [
        name: TName,
        listener: TEventListener<TMap, TName>,
      ] => [
        name,
        (data) => {
          resolve(data);

          listeners.forEach((args) => this.removeListener(...args));
        },
      ]);

      listeners.forEach((args) => this.on(...args));
    });
  }
}
