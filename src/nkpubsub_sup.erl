%% -------------------------------------------------------------------
%%
%% Copyright (c) 2020 Carlos Gonzalez Florido.  All Rights Reserved.
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

%% @private NkDIST LIB main supervisor
-module(nkpubsub_sup).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-behaviour(supervisor).

-export([init/1, start_link/0]).
-export([start_events_sup/0, start_event_server/1]).

-include("nkpubsub.hrl").

%% @private
-spec start_link() ->
    {ok, pid()}.

start_link() ->
    ChildsSpec = [
        {nkpubsub_events_sup,
            {?MODULE, start_events_sup, []},
            permanent,
            infinity,
            supervisor,
            [?MODULE]}
    ],
    supervisor:start_link({local, ?MODULE}, ?MODULE, {{one_for_one, 10, 60}, 
                          ChildsSpec}).


%% @private
init(ChildSpecs) ->
    {ok, ChildSpecs}.


%% @private
start_events_sup() ->
    supervisor:start_link({local, nkpubsub_events_sup}, ?MODULE, {{one_for_one, 10, 60}, []}).


%% @doc
start_event_server(#nkpubsub{topic=Topic, class=Class, type=Type}=Event) ->
    Spec = {
        {Topic, Class, Type},
        {nkpubsub_srv, start_link, [Topic, Class, Type]},
        transient,
        5000,
        worker,
        [nkpubsub_srv]
    },
    case supervisor:start_child(nkpubsub_events_sup, Spec) of
        {ok, undefined} ->
            {error, started_elsewhere};
        {ok, Pid}  ->
            {ok, Pid};
        {error, {already_started, Pid}} ->
            {ok, Pid};
        {error, already_present} ->
            ok = supervisor:delete_child(nkpubsub_events_sup, {Topic, Class, Type}),
            start_event_server(Event);
        {error, Error} ->
            {error, Error}
    end.