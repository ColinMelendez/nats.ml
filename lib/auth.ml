type error =
  | Auth_required
  | Missing_nonce
  | Signing of string

let pp_error ppf = function
  | Auth_required ->
      Format.pp_print_string ppf "the server requires authentication"
  | Missing_nonce ->
      Format.pp_print_string ppf "the server did not provide an auth nonce"
  | Signing message -> Format.fprintf ppf "nonce signing failed: %s" message

type signer = nonce:string -> (string, string) result

type t =
  | Anonymous
  | Token of string
  | User_pass of { user : string; pass : string }
  | Nkey of { nkey : string; sign : signer }
  | Jwt of { jwt : string; nkey : string; sign : signer }

let none = Anonymous
let token value = Token value
let user_pass ~user ~pass = User_pass { user; pass }
let nkey ~nkey ~sign = Nkey { nkey; sign }
let jwt ~jwt ~nkey ~sign = Jwt { jwt; nkey; sign }

let signed ~sign ~make info =
  match Info.nonce info with
  | None -> Error Missing_nonce
  | Some nonce -> (
      match sign ~nonce with
      | Ok signature -> Ok (make signature)
      | Error message -> Error (Signing message))

let connect auth info =
  match auth with
  | Anonymous ->
      if Info.auth_required info then Error Auth_required
      else Ok (Client.Connect.v ())
  | Token value -> Ok (Client.Connect.v ~auth_token:value ())
  | User_pass { user; pass } ->
      Ok (Client.Connect.v ~user ~pass ())
  | Nkey { nkey; sign } ->
      signed ~sign
        ~make:(fun signature -> Client.Connect.v ~nkey ~signature ())
        info
  | Jwt { jwt; nkey; sign } ->
      signed ~sign
        ~make:(fun signature ->
          Client.Connect.v ~jwt ~nkey ~signature ())
        info
