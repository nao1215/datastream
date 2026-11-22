%% Erlang FFI for datastream/internal/ref.gleam.
%%
%% Backed by the process dictionary of the process that allocated the
%% ref, keyed off erlang:unique_integer/1. The handle records that
%% process: every consumer stream returned from `broadcast` must run in
%% it, and touching the ref from any other process raises an error that
%% says so instead of reading an unrelated, empty dictionary.

-module(datastream_ref_ffi).
-export([new_ref/1, get_ref/1, set_ref/2, delete_ref/1]).

%% Values are stored wrapped as `{value, V}`, so a stored Gleam value
%% that is the atom `undefined` is not mistaken for a missing slot.
new_ref(Value) ->
    Key = {datastream_ref, erlang:unique_integer([positive])},
    erlang:put(Key, {value, Value}),
    {Key, self()}.

get_ref(Handle) ->
    case erlang:get(owned_key(Handle, <<"get">>)) of
        {value, Value} -> {ok, Value};
        undefined -> {error, nil}
    end.

set_ref(Handle, Value) ->
    erlang:put(owned_key(Handle, <<"set">>), {value, Value}),
    nil.

delete_ref(Handle) ->
    erlang:erase(owned_key(Handle, <<"delete">>)),
    nil.

owned_key({Key, Owner}, _Function) when Owner =:= self() ->
    Key;
owned_key(_, Function) ->
    erlang:error(#{
        gleam_error => panic,
        message => <<"datastream: a broadcast or unzip consumer must be "
                     "pulled in the process that created it">>,
        module => <<"datastream/internal/ref">>,
        function => Function,
        line => 0
    }).
