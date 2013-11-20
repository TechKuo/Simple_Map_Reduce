open Util;;

let (key, values) = Program.get_input() in
  let convert_to_float (s:string) : float = 
    if (String.contains s '.') then float_of_string s
    else float (int_of_string s) in
  let (grades:float list) = List.rev_map convert_to_float values in
  let (sorted_grades:float list) = List.sort compare grades in 
  let odd (n:int) : bool = (n mod 2)=1 in 
  let (half:int) = ((List.length sorted_grades)/2) in 
  let (median_grade:float) = 
    if (odd (List.length sorted_grades)) 
      then List.nth sorted_grades half
    else let left = List.nth sorted_grades half and 
      right = List.nth sorted_grades (half+1) in 
      (left+.right) /. 2. in
  Program.set_output [(string_of_float median_grade)]