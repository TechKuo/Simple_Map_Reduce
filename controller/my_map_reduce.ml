open Util
open Worker_manager
open Thread_pool

let map_jobs = Hashtbl.create 1000
let map_jobs_lock = Mutex.create ()
let map_results = Hashtbl.create 1000 
let map_results_lock = Mutex.create ()
let reduce_jobs = Hashtbl.create 1000
let reduce_jobs_lock = Mutex.create ()
let reduce_results = Hashtbl.create 1000
let reduce_results_lock = Mutex.create ()
let combine_ht = Hashtbl.create 1000

let tpool = Thread_pool.create 60
let map_jobs_list = ref []
let reduce_jobs_list = ref []

(************************************Helpers**********************************)
let ht_to_list ht = Hashtbl.fold (fun n (k,v) a -> (n,(k,v))::a) ht []

let rec number_kvs kv_list = 
  let rec help kv_list acc n = 
    | [] -> acc
    | h::t -> help t ((n,h)::acc) (n+1) in
  help kv_list [] 0





(************************************Do This**********************************)


let map kv_pairs map_filename : (string * string) list = 
  let mapper_wm = initialize_mappers map_filename in
  let num_kvs = number_kvs kv_pairs in
  let rec assign_jobs num_kv_list = match num_kv_list with
    | [] -> print_string "assign_jobs: done"
    | (num,(key,value)) :: t -> 
      print_string "assign_jobs: assigning head";
      let work () = 
      let worker = Worker_manager.pop_worker mapper_wm in
      (* add job to hashtable of jobs being processed *)
      Mutex.lock map_jobs_lock;
      Hashtbl.add map_jobs num (key,value);
      Mutex.unlock map_jobs_lock;
      print_string "assign_jobs: calculating results";
      let result = Worker_manager.map worker key value in
      print_string "assign_jobs: after calculating results";
      match result with
        | Some(list) -> 
           Worker_manager.push_worker mapper_wm worker;
          (* add successful result to results hashtable *)
          Mutex.lock map_results_lock;
          if (not(Hashtbl.mem map_results num)) then begin
            Hashtbl.add map_results num list;
            Mutex.unlock map_results_lock; 
          (* remove sucessful job from hashtable of jobs being processed *)
            Mutex.lock map_jobs_lock;
            Hashtbl.remove map_jobs num;
            Mutex.unlock map_jobs_lock end
          else Mutex.unlock map_results_lock
        |  None -> ()  in
      Thread_pool.add_work work tpool;
      print_string "assign_jobs: assigning tail";
      assign_jobs t in
    let rec process_jobs kvs =  
      print_string "process_jobs: start";
      assign_jobs kvs;
      Thread.delay(0.1);
      Mutex.lock map_jobs_lock;
      let jobs_length = Hashtbl.length map_jobs in
      Mutex.unlock map_jobs_lock;
      if (jobs_length > 0) then 
        print_string "process_jobs: if branch";
        let new_input = ht_to_list map_jobs in
        process_jobs new_input
      else( 
        print_string "process_jobs: else branch";
        Worker_manager.clean_up_workers mapper_wm)
        Thread_pool.destroy tpool; in
    process_jobs num_kvs;
    let return_map_result ht = Hashtbl.fold (fun k v a -> a@[v]) ht [] in
    let map_result = List.flatten (return_map_result map_results)in
    print_map_results map_result;
    map_result

let combine kv_pairs : (string * string list) list = 
  let rec add_to_ht kv_list = match kv_list with
    | [] -> ()
    | (k,v) :: t -> Hashtbl.add combine_ht k v; add_to_ht t in 
  add_to_ht kv_pairs;
  let return_combine_list ht = 
    let fold_func k v a =
    let elem = k,(Hashtbl.find_all ht k) in
    if not(List.mem elem a) then a@[elem] else a in
    Hashtbl.fold fold_func ht [] in
  let combine_result = return_combine_list combine_ht in
  print_combine_results combine_result;
  combine_result

let reduce kvs_pairs reduce_filename : (string * string list) list =
  let reducer_wm = initialize_reducers reduce_filename in
  let tpool = Thread_pool.create 60 in
  let (@) xs ys = List.rev_append (List.rev xs) ys in
  let ht_to_list ht =
    Hashtbl.fold (fun n (k,v_list) a -> a@[(n,(k,v_list))]) ht [] in
  (* to account for duplicate kv pairs and their results*)
  let rec number_kvs kv_list c = match kv_list with
    |[] -> []
    | (key,value_list) :: t -> [(c,(key,value_list))]@(number_kvs t (c+1)) in
  let num_kvs = number_kvs kvs_pairs 0 in
  let rec assign_jobs num_kv_list = match num_kv_list with
    | [] -> ()
    | (num,(key,value_list)) :: t -> 
      let work () = 
      let worker = Worker_manager.pop_worker reducer_wm in
      (* add job to hashtable of jobs being processed *)
      Mutex.lock reduce_jobs_lock;
      Hashtbl.add reduce_jobs num (key,value_list);
      Mutex.unlock reduce_jobs_lock;
      let result = Worker_manager.reduce worker key value_list in
      match result with
        | Some(list) -> 
           Worker_manager.push_worker reducer_wm worker;
          (* add successful result to results hashtable *)
          if (not(Hashtbl.mem reduce_results num)) then begin
            Mutex.lock reduce_results_lock;
            Hashtbl.add reduce_results num (key,list);
            Mutex.unlock reduce_results_lock; 
          (* remove sucessful job from hashtable of jobs being processed *)
            Mutex.lock reduce_jobs_lock;
            Hashtbl.remove reduce_jobs num;
            Mutex.unlock reduce_jobs_lock end
          else ()
        |  None -> ()  in
      Thread_pool.add_work work tpool;
      assign_jobs t in
    let rec process_jobs kvs =  
      assign_jobs kvs;
      Thread.delay(0.1);
      Mutex.lock reduce_jobs_lock;
      let jobs_length = Hashtbl.length reduce_jobs in
      Mutex.unlock reduce_jobs_lock;
      if (jobs_length > 0) then 
        let new_input = ht_to_list reduce_jobs in
        process_jobs new_input
      else( 
        Worker_manager.clean_up_workers reducer_wm;
        Thread_pool.destroy tpool) in
    process_jobs num_kvs;
    let return_reduce_result ht = Hashtbl.fold (fun n (k,l) a -> a@[(k,l)]) ht [] in
    return_reduce_result reduce_results 
  
let map_reduce app_name mapper_name reducer_name kv_pairs =
  let map_filename    = Printf.sprintf "apps/%s/%s.ml" app_name mapper_name  in
  let reduce_filename = Printf.sprintf "apps/%s/%s.ml" app_name reducer_name in
  let mapped   = map kv_pairs map_filename in
  let combined = combine mapped in
  let reduced  = reduce combined reduce_filename in
  reduced

