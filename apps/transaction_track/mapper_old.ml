open Util;;

module Tbl = Hashtbl 
let coin_tbl = Tbl.create 20

(******************************Helper Functions********************************)
 
let split_at_n (n:int) (lst:string list) : string list * string list= 
  let rec help n (acc,lst) = 
    if n=0 then (acc,lst)
    else match lst with 
      h::t -> help (n-1) ((acc@[h]),t) in 
  help n ([],lst)

let make_pairs (lst:string list) : (string*int) list = 
  let rec help lst acc = 
    match lst with 
      | [] -> acc
      | coin::value::t -> help t (acc@[(coin,(int_of_string value))]) in 
  help lst []

let store_incoins (coins:string list) : unit = 
  let store coin = 
    if (Tbl.mem coin_tbl coin) 
      then Tbl.replace coin_tbl coin ((Tbl.find coin_tbl coin)@[0]) 
    else Tbl.add coin_tbl coin [0] in 
  List.iter store coins 

let store_outcoins (coins:string list) : unit = 
  let coin_value_pairs: (string*int) list = make_pairs coins in 
  let store (coin,v) = 
    if (Tbl.mem coin_tbl coin) 
      then Tbl.replace coin_tbl coin ((Tbl.find coin_tbl coin)@[v])
    else Tbl.add coin_tbl coin [v] in
  List.iter store coin_value_pairs 
 
let parse_transaction (tr:string list) : unit = 
  match tr with incount::outcount::t ->
    let (inoutcoins:(string list)*(string list)) = 
      split_at_n (int_of_string incount) t in
    store_incoins (fst inoutcoins); store_outcoins (snd inoutcoins)

(* splits t into a list of transactions *)
let split_transactions (t:string list) : string list list =
  let rec help num acc tr = 
    if num=0 then acc
    else match tr with
      incount::outcount::t -> 
      let n = (int_of_string incount) + 2*(int_of_string outcount) + 2 in
      let split_off = split_at_n n tr in
      help (num-1) (acc@[(fst split_off)]) (snd split_off) in
  match t with num::tr -> help (int_of_string num) [] tr

let parse_transactions (transactions:string list) : unit = 
  List.iter parse_transaction (split_transactions transactions)  

(********************************Do This Stuff********************************)

let (key,value) = Program.get_input() in
  print_endline "got correct input: now proccessing";
  let (transactions:string list) = Util.split_words value in
  parse_transactions transactions;
  let parse_coins (coin:string) (v:int list) (acc:(string*int list) list) = 
    let (coin_trans:string) = marshal ((int_of_string key),v) in 
    (coin,coin_trans)::acc in
  let output = Tbl.fold parse_coins coin_tbl [] in
  Program.set_output output



  