# Server credential renewal

Server mapping tokens expire after 8760 hours. They are reused on restart and
are not automatically renewed. Schedule renewal before expiration, retaining
the deployment's existing encryption key. Worker tokens are issued separately
and must not replace the server mapping.

Use a maintenance window with all Multica server instances stopped (or workflow
issuance disabled on every instance). Keep the private Hatchet API reachable.
Perform the following from a trusted server maintenance program connected to the
application database, not a browser or daemon:

1. Read the `workflow_hatchet_tenant` row by workspace UUID. Require
   `deletion_state='active'`, and retain its tenant ID and original ciphertext
   in protected memory for compare-and-swap and recovery. Do not renew a pending,
   deleting, failed, or deleted workspace mapping.
2. Log the provisioning user into `POST /api/v1/users/login`, using the existing
   private provisioning email/password and a private cookie jar. Submit
   `{"name":"multica-server","expiresIn":"8760h"}` to
   `POST /api/v1/tenants/{tenant}/api-tokens`. Keep the response token in memory;
   never print it, put it in command-line arguments, or write plaintext to disk.
3. Verify the fresh token can access
   `GET /api/v1/tenants/{tenant}/workflows` using its own bearer authentication.
   Encrypt it with the application's existing `secretbox.Box.Seal`, using the
   same deployment key as the old mapping, then standard-base64 encode it.
4. In a short application database transaction, lock the workspace row using
   the existing workspace creator lock (`FOR KEY SHARE`). Require it still
   exists. Execute this parameterized compare-and-swap, requiring exactly one
   affected row, then commit:

   ```sql
   UPDATE workflow_hatchet_tenant
   SET token_encrypted=$1, updated_at=now()
   WHERE workspace_id=$2 AND tenant_id=$3
     AND deletion_state='active' AND token_encrypted=$4;
   ```

   Bind fresh ciphertext, workspace UUID, tenant ID, and original ciphertext.
   Keep API calls outside this transaction. If the lock/check/update fails,
   revoke only the fresh token via the cookie-authenticated
   `POST /api/v1/api-tokens/{token_id}` and leave the old mapping intact. The
   pinned JWT carries `token_id`; decoding this trusted mint response supplies
   its revocation ID, not a credential authentication decision. If revocation
   fails, retain the token ID/tenant ID as a protected retry receipt.
5. Restart the server with unchanged encryption/keyset/TLS material. Confirm the
   server decrypts the preserved tenant mapping and can inspect that tenant's
   workflows. Then revoke the old token by its own `token_id`, using the private
   user cookie; require HTTP 204. Existing worker credentials remain valid.
6. If verification fails before old-token revocation, stop issuance again and
   compare-and-swap the fresh ciphertext back to the original ciphertext, under
   the same active-workspace checks. Revoke only the failed fresh token. If the
   original token has already expired, mint another fresh server token instead
   of restoring an expired credential.

This procedure requires a trusted maintenance program using the existing Go
secretbox and database libraries; no turnkey renewal CLI is shipped by Task 2a.
Keep server/provisioning tokens out of product APIs and logs. Credential renewal
does not renew, delete, or replace the Hatchet tenant itself.
