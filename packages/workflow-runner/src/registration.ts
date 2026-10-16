import type { DispatcherClient } from '@hatchet-dev/typescript-sdk/protoc/dispatcher';
import { WorkflowError } from '@multica/workflow-sdk';

/** This worker's exported native RPC client; v1 admin Transport does not see Register. */
export function captureWorkerRegistration(rpc: Pick<DispatcherClient, 'register'>, tenant: string, name: string, capture: (id: string) => void): void {
  const register = rpc.register.bind(rpc);
  rpc.register = async (request, options) => {
    if (request.workerName !== name) throw new WorkflowError('workflow_registration_mismatch');
    const result = await register(request, options);
    if (result.tenantId !== tenant || result.workerName !== name || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(result.workerId)) throw new WorkflowError('workflow_registration_mismatch');
    capture(result.workerId);
    return result;
  };
}
