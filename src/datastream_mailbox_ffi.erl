%% Erlang FFI for datastream/erlang/internal/pump.gleam.
%%
%% A mailbox is a process alias owned by the process that created it.
%% Worker processes send `{Alias, Message}` to it. Once the owner calls
%% `close/1` the alias is deactivated, so anything a worker still sends
%% afterwards is dropped by the runtime instead of piling up in the
%% owner's mailbox, and the messages that already arrived are flushed.

-module(datastream_mailbox_ffi).
-export([new/0, send/2, receive_event/3, receive_event_forever/2, close/1,
         raise_worker_crash/2, start_reaper/0, stop_reaper/1, guard/2]).

new() ->
    erlang:alias().

send(Alias, Message) ->
    erlang:send(Alias, {Alias, Message}),
    nil.

%% Waits for a message on `Alias` or a 'DOWN' for one of the monitors
%% that are keys of the `Watched` map, whose value identifies the worker.
receive_event(Alias, Watched, Timeout) ->
    receive
        {Alias, Message} ->
            {received, Message};
        {'DOWN', Monitor, process, _Pid, Reason}
          when is_map_key(Monitor, Watched) ->
            {worker_down, maps:get(Monitor, Watched), Reason}
    after Timeout ->
        timed_out
    end.

receive_event_forever(Alias, Watched) ->
    receive
        {Alias, Message} ->
            {received, Message};
        {'DOWN', Monitor, process, _Pid, Reason}
          when is_map_key(Monitor, Watched) ->
            {worker_down, maps:get(Monitor, Watched), Reason}
    end.

close(Alias) ->
    erlang:unalias(Alias),
    flush(Alias).

flush(Alias) ->
    receive
        {Alias, _} -> flush(Alias)
    after 0 ->
        nil
    end.

%% Re-raises a worker crash in the calling process as a Gleam panic.
raise_worker_crash(Context, Reason) ->
    Message = unicode:characters_to_binary(
        io_lib:format("~ts: a worker process crashed: ~0tp", [Context, Reason])
    ),
    erlang:error(#{
        gleam_error => panic,
        message => Message,
        module => <<"datastream/erlang/internal/pump">>,
        function => <<"raise_worker_crash">>,
        line => 0,
        reason => Reason
    }).

%% A reaper is linked to the tasks of one run and monitors the process
%% that started it. It traps exits so a crashing task does not take it
%% down, and when the starting process exits, or `stop_reaper/1` is
%% called, it exits itself, which kills every task still linked to it.
start_reaper() ->
    Consumer = self(),
    erlang:spawn(fun() ->
        process_flag(trap_exit, true),
        Monitor = erlang:monitor(process, Consumer),
        reaper_loop(Monitor)
    end).

reaper_loop(Monitor) ->
    receive
        {'DOWN', Monitor, process, _, _} -> exit(consumer_down);
        stop -> exit(shutdown);
        {'EXIT', _, _} -> reaper_loop(Monitor)
    end.

stop_reaper(Reaper) ->
    Reaper ! stop,
    nil.

%% Runs `Body`; if it raises, runs `OnCrash` and re-raises unchanged.
guard(Body, OnCrash) ->
    try Body()
    catch Class:Reason:Stacktrace ->
        OnCrash(),
        erlang:raise(Class, Reason, Stacktrace)
    end.
