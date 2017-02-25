%% -------------------------------------------------------------------
%%
%% Copyright (c) 2010-2017 Basho Technologies, Inc.
%% Copyright (c) 2009 Paulo Sérgio Almeida.
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

%% @doc Provides scalable bloom filters that can grow indefinitely while
%%      ensuring a desired maximum false positive probability.
%% Also provides standard partitioned bloom filters with a maximum capacity.
%% Bit arrays are dimensioned as a power of 2 to enable reusing hash values
%% across filters through bit operations. Double hashing is used (no need for
%% enhanced double hashing for partitioned bloom filters).
%%
%% @reference Based on "Scalable Bloom Filters"<br />
%%  Paulo S&#233;rgio Almeida, Carlos Baquero, Nuno Pregui&#231;a, David Hutchison<br />
%%  Information Processing Letters<br />
%%  Volume 101, Issue 6, 31 March 2007, Pages 255-261<br />
%%  [http://haslab.uminho.pt/cbm/files/dbloom.pdf]
%%
%% @end
-module(bloom).

-author("Paulo Sergio Almeida <psa@di.uminho.pt>").
%% Original: https://sites.google.com/site/scalablebloomfilters/
%% Modified slightly by Justin Sheehy to make it a single file
%% (incorporated the array-based bitarray internally).

-export([sbf/1, sbf/2, sbf/3, sbf/4,
         bloom/1, bloom/2,
         member/2, add/2,
         size/1, capacity/1]).

-export([is_element/2, add_element/2]). % alternative names

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% alternative names
is_element(E, B) -> member(E, B).
add_element(E, B) -> add(E, B).

-define(W, 27).

-record(bloom, {
  e,    % error probability
  n,    % maximum number of elements
  mb,   % 2^mb = m, the size of each slice (bitvector)
  size, % number of elements
  a     % list of bitvectors
}).

-record(sbf, {
  e,    % error probability
  r,    % error probability ratio
  s,    % log 2 of growth ratio
  size, % number of elements
  b     % list of plain bloom filters
}).

%% Constructors for (fixed capacity) bloom filters
%%
%% N - capacity
%% E - error probability

bloom(N) -> bloom(N, 0.001).
bloom(N, E) when is_number(N), N > 0,
            is_float(E), E > 0, E < 1,
            N >= 4/E -> % rule of thumb; due to double hashing
  bloom(size, N, E).

bloom(Mode, Dim, E) ->
  K = 1 + trunc(log2(1/E)),
  P = math:pow(E, 1 / K),
  case Mode of
    size -> Mb = 1 + trunc(-log2(1 - math:pow(1 - P, 1 / Dim)));
    bits -> Mb = Dim
  end,
  M = 1 bsl Mb,
  N = trunc(math:log(1-P) / math:log(1-1/M)),
  #bloom{e=E, n=N, mb=Mb, size = 0,
         a = [bitarray_new(1 bsl Mb) || _ <- lists:seq(1, K)]}.

log2(X) -> math:log(X) / math:log(2).

%% Constructors for scalable bloom filters
%%
%% N - initial capacity before expanding
%% E - error probability
%% S - growth ratio when full (log 2) can be 1, 2 or 3
%% R - tightening ratio of error probability

sbf(N) -> sbf(N, 0.001).
sbf(N, E) -> sbf(N, E, 1).
sbf(N, E, 1) -> sbf(N, E, 1, 0.85);
sbf(N, E, 2) -> sbf(N, E, 2, 0.75);
sbf(N, E, 3) -> sbf(N, E, 3, 0.65).
sbf(N, E, S, R) when is_number(N), N > 0,
                     is_float(E), E > 0, E < 1,
                     is_integer(S), S > 0, S < 4,
                     is_float(R), R > 0, R < 1,
                     N >= 4/(E*(1-R)) -> % rule of thumb; due to double hashing
  #sbf{e=E, s=S, r=R, size=0, b=[bloom(N, E*(1-R))]}.

%% Returns number of elements
%%
size(#bloom{size=Size}) -> Size;
size(#sbf{size=Size}) -> Size.

%% Returns capacity
%%
capacity(#bloom{n=N}) -> N;
capacity(#sbf{}) -> infinity.

%% Test for membership
%%
member(Elem, #bloom{mb=Mb}=B) ->
  Hashes = make_hashes(Mb, Elem),
  hash_member(Hashes, B);
member(Elem, #sbf{b=[H|_]}=Sbf) ->
  Hashes = make_hashes(H#bloom.mb, Elem),
  hash_member(Hashes, Sbf).

hash_member(Hashes, #bloom{mb=Mb, a=A}) ->
  Mask = 1 bsl Mb -1,
  {I1, I0} = make_indexes(Mask, Hashes),
  all_set(Mask, I1, I0, A);
hash_member(Hashes, #sbf{b=B}) ->
  lists:any(fun(X) -> hash_member(Hashes, X) end, B).

make_hashes(Mb, E) when Mb =< 16 ->
  erlang:phash2({E}, 1 bsl 32);
make_hashes(Mb, E) when Mb =< 32 ->
  {erlang:phash2({E}, 1 bsl 32), erlang:phash2([E], 1 bsl 32)}.

make_indexes(Mask, {H0, H1}) when Mask > 1 bsl 16 -> masked_pair(Mask, H0, H1);
make_indexes(Mask, {H0, _}) -> make_indexes(Mask, H0);
make_indexes(Mask, H0) -> masked_pair(Mask, H0 bsr 16, H0).

masked_pair(Mask, X, Y) -> {X band Mask, Y band Mask}.

all_set(_Mask, _I1, _I, []) -> true;
all_set(Mask, I1, I, [H|T]) ->
  case bitarray_get(I, H) of
    true -> all_set(Mask, I1, (I+I1) band Mask, T);
    false -> false
  end.

%% Adds element to set
%%
add(Elem, #bloom{mb=Mb} = B) ->
  Hashes = make_hashes(Mb, Elem),
  hash_add(Hashes, B);
add(Elem, #sbf{size=Size, r=R, s=S, b=[H|T]=Bs}=Sbf) ->
  #bloom{mb=Mb, e=E, n=N, size=HSize} = H,
  Hashes = make_hashes(Mb, Elem),
  case hash_member(Hashes, Sbf) of
    true -> Sbf;
    false ->
      case HSize < N of
        true -> Sbf#sbf{size=Size+1, b=[hash_add(Hashes, H)|T]};
        false ->
          B = add(Elem, bloom(bits, Mb + S, E * R)),
          Sbf#sbf{size=Size+1, b=[B|Bs]}
      end
  end.

hash_add(Hashes, #bloom{mb=Mb, a=A, size=Size} = B) ->
  Mask = 1 bsl Mb -1,
  {I1, I0} = make_indexes(Mask, Hashes),
  case all_set(Mask, I1, I0, A) of
    true -> B;
    false -> B#bloom{size=Size+1, a=set_bits(Mask, I1, I0, A, [])}
  end.

set_bits(_Mask, _I1, _I, [], Acc) -> lists:reverse(Acc);
set_bits(Mask, I1, I, [H|T], Acc) ->
  set_bits(Mask, I1, (I+I1) band Mask, T, [bitarray_set(I, H) | Acc]).

bitarray_new(N) -> array:new((N-1) div ?W + 1, {default, 0}).

bitarray_set(I, A) ->
  AI = I div ?W,
  V = array:get(AI, A),
  V1 = V bor (1 bsl (I rem ?W)),
  array:set(AI, V1, A).

bitarray_get(I, A) ->
  AI = I div ?W,
  V = array:get(AI, A),
  V band (1 bsl (I rem ?W)) =/= 0.

-ifdef(TEST).

simple_shuffle(L, N) ->
    lists:sublist(simple_shuffle(L), 1, N).
simple_shuffle(L) ->
    RandMod = riak_core_util:rand_module(),
    N = 1000 * length(L),
    L2 = [{RandMod:uniform(N), E} || E <- L],
    {_, L3} = lists:unzip(lists:keysort(1, L2)),
    L3.

fixed_case_test_() ->
    {timeout, 100, fun() -> fixed_case(bloom(5000), 5000, 0.001) end}.

fixed_case(Bloom, Size, FalseRate) ->
    ?assert(bloom:capacity(Bloom) > Size),
    ?assertEqual(0, bloom:size(Bloom)),
    RandomList = simple_shuffle(lists:seq(1,100*Size), Size),
    [?assertEqual(false, bloom:is_element(E, Bloom)) || E <- RandomList],
    Bloom2 =
        lists:foldl(fun(E, Bloom0) ->
                            bloom:add_element(E, Bloom0)
                    end, Bloom, RandomList),
    [?assertEqual(true, bloom:is_element(E, Bloom2)) || E <- RandomList],

    ?assert(bloom:size(Bloom2) > ((1-FalseRate)*Size)),
    ok.

scalable_case(Bloom, Size, FalseRate) ->
    ?assertEqual(infinity, bloom:capacity(Bloom)),
    ?assertEqual(0, bloom:size(Bloom)),
    RandomList = simple_shuffle(lists:seq(1,100*Size), 10*Size),
    [?assertEqual(false, bloom:is_element(E, Bloom)) || E <- RandomList],
    Bloom2 =
        lists:foldl(fun(E, Bloom0) ->
                            bloom:add_element(E, Bloom0)
                    end, Bloom, RandomList),
    [?assertEqual(true, bloom:is_element(E, Bloom2)) || E <- RandomList],
    ?assert(bloom:size(Bloom2) > ((1-FalseRate)*Size)),
    ok.

bloom_test() ->
    scalable_case(sbf(1000, 0.2), 1000, 0.2),
    ok.

-endif.
