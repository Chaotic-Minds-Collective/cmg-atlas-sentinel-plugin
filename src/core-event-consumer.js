import { evaluateSentinelEvent, SENTINEL_DEFAULT_RULES } from "./index.js";
const DEFAULT_TIMEOUT_MS=5000, MAX_TIMEOUT_MS=30000, MAX_CLAIM_LIMIT=100, MIN_LEASE_SECONDS=15, MAX_LEASE_SECONDS=300;
const UUID=/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
export const SENTINEL_OUTBOX_EVENT_TYPES=Object.freeze([...new Set(SENTINEL_DEFAULT_RULES.map((rule)=>rule.eventType))]);
function nonEmpty(value,field){if(typeof value!=="string"||value.trim()==="")throw new TypeError(field+" must be a non-empty string");return value.trim();}
function uuid(value,field){const result=nonEmpty(value,field);if(!UUID.test(result))throw new TypeError(field+" must be a UUID");return result;}
function bounded(value,field,min,max){if(!Number.isSafeInteger(value)||value<min||value>max)throw new RangeError(field+" must be an integer from "+min+" to "+max);return value;}
function httpsOrigin(value){const raw=nonEmpty(value,"baseUrl");let url;try{url=new URL(raw);}catch{throw new TypeError("baseUrl must be an HTTPS origin");}if(url.protocol!=="https:"||url.username||url.password||url.pathname!=="/"||url.search||url.hash)throw new TypeError("baseUrl must be an HTTPS origin without credentials, path, query, or fragment");return url.origin;}
async function readData(response){if(!response||typeof response.ok!=="boolean"||typeof response.status!=="number")throw new TypeError("Core response must be a Fetch API Response");let body;try{body=await response.json();}catch{throw new TypeError("Core response returned invalid JSON");}if(!response.ok){const error=new Error(typeof body?.error?.message==="string"?body.error.message:"Core request failed");error.name="CoreOutboxRequestError";error.status=response.status;error.code=typeof body?.error?.code==="string"?body.error.code:"CORE_REQUEST_FAILED";throw error;}if(!body||typeof body!=="object"||Array.isArray(body)||!Object.hasOwn(body,"data"))throw new TypeError("Core response must contain a data field");return body.data;}
function normalizeClaim(row,serverId,resolveScope,index){
 if(!row||typeof row!=="object"||Array.isArray(row))throw new TypeError("Core claim response data["+index+"] must be an object");
 const outboxId=uuid(row.id,"claimed event id"),leaseToken=uuid(row.leaseToken,"claimed event leaseToken"),payload=row.payload;
 if(row.eventType!=="game.event.received"||!payload||typeof payload!=="object"||Array.isArray(payload)||payload.serverId!==serverId)throw new TypeError("Core claim response contains an event outside the requested server scope");
 if(!SENTINEL_OUTBOX_EVENT_TYPES.includes(payload.eventType))throw new TypeError("Core claim response contains an unsupported Sentinel alert event type");
 const resolved=resolveScope(Object.freeze({...payload}));
 if(!resolved||typeof resolved!=="object"||Array.isArray(resolved))throw new TypeError("resolveScope must return explicit clusterId, gridId, and serverId");
 const scope=Object.freeze({clusterId:nonEmpty(resolved.clusterId,"scope.clusterId"),gridId:nonEmpty(resolved.gridId,"scope.gridId"),serverId:uuid(resolved.serverId,"scope.serverId")});
 if(scope.clusterId!==payload.clusterId||scope.serverId!==serverId)throw new TypeError("resolved scope must match Core clusterId and serverId");
 const event=Object.freeze({schemaVersion:1,eventId:nonEmpty(payload.eventId,"payload.eventId"),eventType:payload.eventType,occurredAt:nonEmpty(payload.occurredAt,"payload.occurredAt"),scope,payload:payload.payload&&typeof payload.payload==="object"&&!Array.isArray(payload.payload)?payload.payload:{}});
 const alerts=evaluateSentinelEvent(event);
 return Object.freeze({outboxEvent:Object.freeze({id:outboxId,leaseToken}),event,alerts});
}
/** Explicitly invoked read-only Sentinel consumer. It never polls or acknowledges automatically. */
export function createSentinelCoreEventConsumer(options){
 if(!options||typeof options!=="object"||Array.isArray(options))throw new TypeError("options must be an object");
 const baseUrl=httpsOrigin(options.baseUrl),serviceToken=nonEmpty(options.serviceToken,"serviceToken"),serverId=uuid(options.serverId,"serverId");
 if(typeof options.resolveScope!=="function")throw new TypeError("resolveScope must map each Core event to explicit cluster/grid/server IDs");
 const resolveScope=options.resolveScope,timeoutMs=options.timeoutMs??DEFAULT_TIMEOUT_MS;bounded(timeoutMs,"timeoutMs",100,MAX_TIMEOUT_MS);
 const fetchImpl=options.fetchImpl??globalThis.fetch;if(typeof fetchImpl!=="function")throw new TypeError("fetchImpl must be a function");
 async function request(path,body){const response=await fetchImpl(baseUrl+path,{method:"POST",headers:{accept:"application/json","content-type":"application/json",authorization:"Bearer "+serviceToken},body:JSON.stringify(body),cache:"no-store",redirect:"error",signal:AbortSignal.timeout(timeoutMs)});return readData(response);}
 async function claim(claimOptions={}){
  if(!claimOptions||typeof claimOptions!=="object"||Array.isArray(claimOptions))throw new TypeError("claim options must be an object");
  const limit=claimOptions.limit??25,leaseSeconds=claimOptions.leaseSeconds??120;bounded(limit,"limit",1,MAX_CLAIM_LIMIT);bounded(leaseSeconds,"leaseSeconds",MIN_LEASE_SECONDS,MAX_LEASE_SECONDS);
  const path="/integrations/servers/"+encodeURIComponent(serverId)+"/events/claim";
  const data=await request(path,{limit,leaseSeconds,eventTypes:SENTINEL_OUTBOX_EVENT_TYPES});if(!Array.isArray(data))throw new TypeError("Core claim response data must be an array");
  return Object.freeze(data.map((row,index)=>normalizeClaim(row,serverId,resolveScope,index)));
 }
 async function acknowledge(claimedEvent,outcome,errorMessage){
  if(!claimedEvent||typeof claimedEvent!=="object"||!claimedEvent.outboxEvent)throw new TypeError("claimedEvent must be returned by claim()");
  const eventId=uuid(claimedEvent.outboxEvent.id,"claimedEvent.outboxEvent.id"),leaseToken=uuid(claimedEvent.outboxEvent.leaseToken,"claimedEvent.outboxEvent.leaseToken");
  if(outcome!=="delivered"&&outcome!=="failed")throw new TypeError("outcome must be delivered or failed");
  if(errorMessage!==undefined&&(typeof errorMessage!=="string"||errorMessage.length>1000))throw new TypeError("error must be a string no longer than 1000 characters");
  if(outcome==="delivered"&&errorMessage!==undefined)throw new TypeError("error is only valid when outcome is failed");
  const body={leaseToken,outcome};if(errorMessage!==undefined)body.error=errorMessage;
  const path="/integrations/servers/"+encodeURIComponent(serverId)+"/events/"+encodeURIComponent(eventId)+"/ack";const data=await request(path,body);
  if(!data||data.accepted!==true||data.eventId!==eventId)throw new TypeError("Core acknowledgement response did not confirm the requested event");return Object.freeze({...data});
 }
 return Object.freeze({claim,acknowledge,serverId});
}
