%% Erlang FFI for datastream/internal/ref.gleam.
%%
%% Backed by the process dictionary of the process that allocated the
%% ref, keyed off erlang:unique_integer/1. The handle records that
%% process: every consumer stream returned from `broadcast` must run in
%% it, and touching the ref from any other process raises an error that
%% says so instead of reading an unrelated, empty dictionary.

-module(datastream_ref_ffi).
-export([new_ref/1, get_ref/1, set_ref/2, delete_ref/1]).

new_ref(Value) ->
    Key = {datastream_ref, erlang:unique_integer([positive])},
    erlang:put(Key, Value),
    {Key, self()}.

get_ref(Handle) ->
    case erlang:get(owned_key(Handle)) of
        undefined -> {error, nil};
        Value -> {ok, Value}
    end.

set_ref(Handle, Value) ->
    erlang:put(owned_key(Handle), Value),
    nil.

delete_ref(Handle) ->
    erlang:erase(owned_key(Handle)),
    nil.

owned_key({Key, Owner}) when Owner =:= self() ->
    Key;
owned_key(_) ->
    erlang:error(#{
        gleam_error => panic,
        message => <<"datastream: a broadcast or unzip consumer must be "
                     "pulled in the process that created it">>,
        module => <<"datastream/internal/ref">>,
        function => <<"get">>,
        line => 0
    }).
