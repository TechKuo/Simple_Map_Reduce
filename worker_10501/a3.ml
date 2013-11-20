open Util;;
let (key, value) = Program.get_input() in
let class_grades = 
  let classes = (split_to_class_lst value) in
  let map_fun c = match (split_spaces c) with  k::v::t -> (k,v) in 
  List.rev_map map_fun classes in
Program.set_output class_grades

