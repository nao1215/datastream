%% Erlang FFI for datastream/erlang/internal/pump.gleam.
%%
%% A mailbox is a process alias owned by the process that created it.
%% Worker processes send `{Alias, Message}` to it. Once the owner calls
%% `close/1` the alias is deactivated, so anything a worker still sends
%% afterwards is dropped by the runtime instead of piling up in the
%% owner's mailbox, and the messages that already arrived are flushed.

-module(datastream_mailbox_ffi).
-export([new/0, send/2, receive_event/3, receive_event_forever/2, close/1,
         raise_worker_crash/2]).

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
