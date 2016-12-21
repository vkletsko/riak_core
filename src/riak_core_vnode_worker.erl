%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(riak_core_vnode_worker).

-behaviour(gen_server).

-export([behaviour_info/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
        code_change/3]).
-export([start_link/1, handle_work/3, handle_work/4]).

-ifdef(PULSE).
-compile(export_all).
-compile({parse_transform, pulse_instrument}).
-compile({pulse_replace_module, [{gen_fsm, pulse_gen_fsm},
                                 {gen_server, pulse_gen_server}]}).
-endif.

-record(state, {
        module :: atom(),
        modstate :: any(),
        caller :: pid(),
        task_pid :: pid()
    }).

-spec behaviour_info(atom()) -> 'undefined' | [{atom(), arity()}].
behaviour_info(callbacks) ->
    [{init_worker,3},
     {handle_work,3}];
behaviour_info(_Other) ->
    undefined.

start_link(Args) ->
    WorkerMod = proplists:get_value(worker_callback_mod, Args),
    [VNodeIndex, WorkerArgs, WorkerProps, Caller] = proplists:get_value(worker_args, Args),
    gen_server:start_link(?MODULE, [WorkerMod, VNodeIndex, WorkerArgs, WorkerProps, Caller], []).

handle_work(Worker, Work, From) ->
    handle_work(Worker, Work, From, self()).

handle_work(Worker, Work, From, Caller) ->
    gen_server:cast(Worker, {work, Work, From, Caller}).

work_done(Worker, ModState) ->
    gen_server:cast(Worker, {work_done, ModState}).

init([Module, VNodeIndex, WorkerArgs, WorkerProps, Caller]) ->
    {ok, WorkerState} = Module:init_worker(VNodeIndex, WorkerArgs, WorkerProps),
    %% let the pool queue manager know there might be a worker to checkout
    riak_core_vnode_worker_pool:start_worker(Caller),
    {ok, #state{module=Module, modstate=WorkerState}}.

handle_call(Event, _From, State) ->
    lager:debug("Vnode worker received synchronous event: ~p.", [Event]),
    {reply, ok, State}.

handle_cast({work, Work, WorkFrom, Caller}, State) ->
    NewState = spawn_task(self(), Work, WorkFrom, State),
    {noreply, NewState#state{caller=Caller}};
handle_cast({work_done, NewModState}, #state{caller=Caller}=State) ->
    %% check the worker back into the pool
    riak_core_vnode_worker_pool:checkin_worker(Caller, self()),
    {noreply, State#state{modstate=NewModState, caller=undefined, task_pid=undefined}};
handle_cast(_Event, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

spawn_task(WorkerPid, Work, WorkFrom, #state{modstate=ModState, module=Mod}= State) ->
    TaskPid= spawn_link( fun() ->
                                 execute_work(WorkerPid, Work, WorkFrom, ModState, Mod)
                         end ),
    State#state{task_pid = TaskPid}.

execute_work(WorkerPid, Work, WorkFrom, ModState, Mod) ->
    NewModState = 
        case Mod:handle_work(Work, WorkFrom, ModState) of
            {reply, Reply, NS} ->
                riak_core_vnode:reply(WorkFrom, Reply),
                NS;
            {noreply, NS} ->
                NS
        end,
    work_done(WorkerPid, NewModState).
    
