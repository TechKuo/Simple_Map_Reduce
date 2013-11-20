open Util;;
open Int64;;

let (key,value) = Program.get_input() in
  print_endline ("key: "^key^"value: "^value);
  let transactions = Util.split_words value in
  let coin_tbl = Hashtbl.create 50 in 
  let rec process_transactions transactions =
  let split_at_n n lst = 
    let rec help n (acc,lst) = 
    if n=0 then (acc,lst)
    else match lst with
    | []   -> failwith "n > len(lst)" 
    | h::t -> help (n-1) ((acc@[h]),t) in 
    help n ([],lst) in 
  match transactions with 
    | c_in::c_out::t ->  
      let in_num         = (int_of_string c_in)  in 
      let out_num        = (int_of_string c_out) in
      let split_incoins  = split_at_n in_num t   in
      let incoins        = fst split_incoins     in
      let split_outcoins = split_at_n (2*out_num) (snd split_incoins) in 
      let outcoins       = fst split_outcoins    in 
      let next_trans     = snd split_outcoins    in
      let make_pairs l   = 
        let rec help l acc   = match l with
        | k::v::t -> help t (acc@[(k,(of_string v))]) | _ -> acc in 
        help l [] in 
      List.iter (fun c -> Hashtbl.add coin_tbl c 0L) incoins;
      List.iter (fun (k,v) -> (Hashtbl.add coin_tbl k v)) (make_pairs outcoins);
      process_transactions next_trans
    | _ -> () in 
  let do_stuff = match transactions with 
    | []   -> () 
    | h::t -> process_transactions t in 
  let output = 
    Hashtbl.fold (fun k v acc ->
      let trans_history = marshal ((int_of_string key),
      (List.rev (Hashtbl.find_all coin_tbl k))) in 
      let elem = (k,trans_history) in 
      if (List.mem elem acc) then acc else elem::acc) coin_tbl [] in 
  print_endline "returning map result";
  Program.set_output output
  