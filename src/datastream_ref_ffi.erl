%% Erlang FFI for datastream/internal/ref.gleam.
%%
%% Backed by the calling process's process dictionary, keyed off
%% erlang:unique_integer/1. The dictionary is per-process, which is
%% fine for datastream's single-process pipeline use: every consumer
%% stream returned from `broadcast` runs inside the same process as
%% the call to broadcast, so they all see the same dict.

-module(datastream_ref_ffi).
-export([new_ref/1, get_ref/1, set_ref/2]).

new_ref(Value) ->
    Key = erlang:unique_integer([positive]),
    erlang:put({datastream_ref, Key}, Value),
    Key.

get_ref(Key) ->
    erlang:get({datastream_ref, Key}).

set_ref(Key, Value) ->
    erlang:put({datastream_ref, Key}, Value),
    nil.
