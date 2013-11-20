open Protocol

let mapper_set = Hashtbl.create 100 
let ms_lock = Mutex.create () 
let reducer_set = Hashtbl.create 100 
let rs_lock = Mutex.create ()

let send_response client response =
  let success = Connection.output client response in
    (if not success then
      (Connection.close client;
       print_endline "Connection lost before response could be sent.")
    else ());
    success

let rec handle_request client =
  match Connection.input client with
    Some v ->
      begin
        match v with
        | InitMapper source -> begin 
          let mapper_build = Program.build source in
          match mapper_build with
          | (Some(id),_) -> begin 
            Mutex.lock ms_lock;
            Hashtbl.add mapper_set (Hashtbl.hash id) id;
            Mutex.unlock ms_lock;
            let send_attempt = send_response client (Mapper (Some(id),"")) in
            if send_attempt then handle_request client else Connection.close client end
          | (None,error_msg) -> begin 
            let send_attempt = send_response client (Mapper (None,error_msg)) in
            if send_attempt then Connection.close client else Connection.close client end end
        | InitReducer source -> begin 
          let reducer_build = Program.build source in
          match reducer_build with 
          | (Some id,_) -> begin
            Mutex.lock rs_lock;
            Hashtbl.add reducer_set (Hashtbl.hash id) id;
            Mutex.unlock rs_lock;
            let send_attempt = send_response client (Reducer (Some id, "")) in
            if send_attempt then handle_request client else Connection.close client end
          | (None,error_msg) -> begin
            let send_attempt = send_response client (Reducer (None,error_msg)) in
            if send_attempt then Connection.close client else Connection.close client end end
        | MapRequest (id, k, v) -> begin 
          Mutex.lock ms_lock;
          let id_is_mem = Hashtbl.mem mapper_set (Hashtbl.hash id) in
          Mutex.unlock ms_lock;
          if id_is_mem then begin
            match (Program.run id (k,v)) with
            | Some(result) -> begin
              let send_attempt = send_response client (MapResults (id,result)) in
              if send_attempt then handle_request client else Connection.close client end
            | None -> begin
              let send_attempt = 
              send_response client (RuntimeError (id, "MapRequest Error")) in
              if send_attempt then Connection.close client else Connection.close client end end
          else begin
              let send_attempt = send_response client (InvalidWorker id) in
              if send_attempt then Connection.close client else Connection.close client end end
        | ReduceRequest (id, k, v) -> begin 
          Mutex.lock rs_lock;
          let id_is_mem = Hashtbl.mem reducer_set (Hashtbl.hash id) in
          Mutex.unlock rs_lock;
          if id_is_mem then begin
            match (Program.run id (k,v)) with
            | Some(result) -> begin
              let send_attempt = 
              send_response client (ReduceResults (id,result)) in
              if send_attempt then handle_request client else Connection.close client end
            | None -> begin
              let send_attempt =  
              send_response client (RuntimeError (id, "ReduceRequest Error")) in
              if send_attempt then Connection.close client else Connection.close client end end
          else begin 
              let send_attempt = send_response client (InvalidWorker id ) in
              if send_attempt then Connection.close client else Connection.close client end end
      end
  | None ->
      Connection.close client;
      print_endline "Connection lost while waiting for request."

