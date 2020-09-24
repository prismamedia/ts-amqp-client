import { EventEmitter } from 'events';
import { clearTimeout, setTimeout } from 'timers';

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
    name: TName,
    timeoutInMs: number = 60000,
  ): Promise<TEventData<TMap, TName>> {
    return new Promise((resolve, reject) => {
      const timeoutId = setTimeout(
        () =>
          reject(new Error(`The timeout of ${timeoutInMs}ms has been reached`)),
        timeoutInMs,
      );

      this.once(name, (data) => {
        clearTimeout(timeoutId);

        resolve(data);
      });
    });
  }
}
