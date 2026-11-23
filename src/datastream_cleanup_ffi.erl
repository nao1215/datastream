%% Erlang FFI for datastream/internal/cleanup.gleam.
%%
%% A cell holds the cleanup of one open resource or worker group. A
%% frame lists the cells that should be cleaned up if the code running
%% inside it raises. Both live in the process dictionary of the process
%% that created them:
%%
%%   datastream_frames             => [FrameId, ...], innermost first
%%   {datastream_frame, FrameId}   => #{CellKey => Seq}
%%   CellKey                       => {Cleanup, FrameId | none}
%%
%% CellKey is {datastream_cell, Unique, OwnerPid}. A cell that has run
%% or been discarded has no dictionary entry.

-module(datastream_cleanup_ffi).
-export([new_cell/1, set_cell/2, touch_cell/1, take_cell/2, discard_cell/1,
         guard/1, new_frame/0, guard_in/3]).

new_cell(Cleanup) ->
    Key = {datastream_cell, erlang:unique_integer([positive]), self()},
    put(Key, {Cleanup, none}),
    register_cell(Key),
    Key.

%% Called on every resource pull, so it reads the frame stack once and
%% writes the cell once; the old value returned by put/2 tells whether
%% the cell changed frames or had already been taken.
set_cell(Key, Cleanup) ->
    case owned(Key) of
        true ->
            Frame = case get(datastream_frames) of
                [Innermost | _] -> Innermost;
                undefined -> none
            end,
            case put(Key, {Cleanup, Frame}) of
                {_, Frame} ->
                    nil;
                {_, none} when Frame =:= none ->
                    nil;
                {_, Old} ->
                    put(Key, {Cleanup, Old}),
                    register_cell(Key);
                undefined ->
                    erase(Key)
            end;
        false ->
            nil
    end,
    nil.

touch_cell(Key) ->
    case owned(Key) andalso get(Key) of
        {_, _} -> register_cell(Key);
        _ -> nil
    end,
    nil.

%% Runs the cell's cleanup once. A cell owned by another process cannot
%% be read here, so the caller's own cleanup runs instead.
take_cell(Key, Fallback) ->
    case owned(Key) of
        false ->
            Fallback();
        true ->
            case erase(Key) of
                {Cleanup, Frame} ->
                    unregister_cell(Key, Frame),
                    Cleanup();
                undefined ->
                    nil
            end
    end.

discard_cell(Key) ->
    case owned(Key) andalso erase(Key) of
        {_, Frame} -> unregister_cell(Key, Frame);
        _ -> nil
    end,
    nil.

%% Runs Body in a new frame. On a normal return, cells still registered
%% in the frame move to the enclosing frame. If Body raises, every cell
%% in the frame is cleaned up, newest first, and the exception
%% continues unchanged.
guard(Body) ->
    Frame = erlang:unique_integer([positive]),
    Outer = frames(),
    put(datastream_frames, [Frame | Outer]),
    try Body() of
        Value ->
            restore(Outer),
            adopt(Frame),
            Value
    catch
        Class:Reason:Stacktrace ->
            restore(Outer),
            clean(Frame),
            erlang:raise(Class, Reason, Stacktrace)
    end.

new_frame() ->
    erlang:unique_integer([positive]).

%% Like guard/1 for a frame that outlives the call: on a normal return
%% its cells stay in it. If Body raises, its cells are cleaned up, then
%% OnRaise runs, and the exception continues.
guard_in(Frame, Body, OnRaise) ->
    Outer = frames(),
    put(datastream_frames, [Frame | Outer]),
    try Body() of
        Value ->
            restore(Outer),
            Value
    catch
        Class:Reason:Stacktrace ->
            restore(Outer),
            clean(Frame),
            quietly(OnRaise),
            erlang:raise(Class, Reason, Stacktrace)
    end.

owned({datastream_cell, _, Owner}) -> Owner =:= self().

frames() ->
    case get(datastream_frames) of
        undefined -> [];
        Frames -> Frames
    end.

restore([]) -> erase(datastream_frames);
restore(Frames) -> put(datastream_frames, Frames).

register_cell(Key) ->
    case frames() of
        [Frame | _] ->
            case get(Key) of
                {_, Frame} ->
                    nil;
                {Cleanup, Old} ->
                    unregister_cell(Key, Old),
                    put(Key, {Cleanup, Frame}),
                    Cells = frame_cells(Frame),
                    Seq = erlang:unique_integer([monotonic]),
                    put({datastream_frame, Frame}, Cells#{Key => Seq})
            end;
        [] ->
            nil
    end.

unregister_cell(_, none) ->
    nil;
unregister_cell(Key, Frame) ->
    case get({datastream_frame, Frame}) of
        undefined ->
            nil;
        Cells ->
            case maps:remove(Key, Cells) of
                Empty when map_size(Empty) =:= 0 ->
                    erase({datastream_frame, Frame});
                Rest ->
                    put({datastream_frame, Frame}, Rest)
            end
    end.

frame_cells(Frame) ->
    case get({datastream_frame, Frame}) of
        undefined -> #{};
        Cells -> Cells
    end.

%% Called after the frame stack is restored, so register_cell/1 puts
%% each surviving cell in the enclosing frame, if there is one.
adopt(Frame) ->
    Cells = frame_cells(Frame),
    erase({datastream_frame, Frame}),
    lists:foreach(
        fun(Key) ->
            case get(Key) of
                {Cleanup, Frame} ->
                    put(Key, {Cleanup, none}),
                    register_cell(Key);
                _ ->
                    nil
            end
        end,
        ordered(Cells, fun(A, B) -> A =< B end)
    ),
    nil.

clean(Frame) ->
    Cells = frame_cells(Frame),
    erase({datastream_frame, Frame}),
    lists:foreach(
        fun(Key) ->
            case erase(Key) of
                {Cleanup, _} -> quietly(Cleanup);
                undefined -> nil
            end
        end,
        ordered(Cells, fun(A, B) -> A >= B end)
    ).

ordered(Cells, Before) ->
    Sorted = lists:sort(fun({_, A}, {_, B}) -> Before(A, B) end,
                        maps:to_list(Cells)),
    [Key || {Key, _} <- Sorted].

quietly(Fun) ->
    try Fun() catch _:_ -> nil end,
    nil.
