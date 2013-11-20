open Util;;
open Int64;;

let (key, values) = Program.get_input() in
  let block_compare (b1,v1) (b2,v2) = compare b1 b2 in
  let flatten sorted_trans = 
    let fold_fun acc (b,v) = acc@v in 
    List.fold_left fold_fun [] sorted_trans in
  let calc_final_value trans_list = 
    let fold_fun acc elem = 
    if elem=0L then 0L else add acc elem in
    List.fold_left fold_fun 0L trans_list in 
  let coin_trans = List.map unmarshal values in
  let sorted_trans = List.sort block_compare coin_trans in
  let trans_list = flatten sorted_trans in 
  let output = to_string (calc_final_value trans_list) in
  Program.set_output [output]