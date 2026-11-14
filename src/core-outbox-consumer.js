import { normalizeCoreTribeEvent } from "./core-event.js";
const DEFAULT_TIMEOUT_MS=5000, MAX_TIMEOUT_MS=30000, MAX_CLAIM_LIMIT=100, MIN_LEASE_SECONDS=15, MAX_LEASE_SECONDS=300;
const UUID=/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
export const TRIBE_OUTBOX_EVENT_TYPES=Object.freeze(["tribe.created","tribe.member.joined","tribe.member.left","tribe.updated"]);
const ALLOWED_TYPES=new Set(TRIBE_OUTBOX_EVENT_TYPES);
function nonEmpty(value,field){if(typeof value!=="string"||value.trim()==="")throw new TypeError(field+" must be a non-empty string");return value.trim();}
function uuid(value,field){const result=nonEmpty(value,field);if(!UUID.test(result))throw new TypeError(field+" must be a UUID");return result;}
function bounded(value,field,min,max){if(!Number.isSafeInteger(value)||value<min||value>max)throw new RangeError(field+" must be an integer from "+min+" to "+max);return value;}
function origin(value){const raw=nonEmpty(value,"baseUrl");let url;try{url=new URL(raw);}catch{throw new TypeError("baseUrl must be an HTTPS origin");}if(url.protocol!=="https:"||url.username||url.password||url.pathname!=="/"||url.search||url.hash)throw new TypeError("baseUrl must be an HTTPS origin without credentials, path, query, or fragment");return url.origin;}
async function readData(response){if(!response||typeof response.ok!=="boolean"||typeof response.status!=="number")throw new TypeError("Core response must be a Fetch API Response");let body;try{body=await response.json();}catch{throw new TypeError("Core response returned invalid JSON");}if(!response.ok){const error=new Error(typeof body?.error?.message==="string"?body.error.message:"Core request failed");error.name="CoreOutboxRequestError";error.status=response.status;error.code=typeof body?.error?.code==="string"?body.error.code:"CORE_REQUEST_FAILED";throw error;}if(!body||typeof body!=="object"||Array.isArray(body)||!Object.hasOwn(body,"data"))throw new TypeError("Core response must contain a data field");return body.data;}
/** Explicitly invoked tribe-only client. Claim and acknowledgement are never automatic. */
export function createCoreTribeOutboxConsumer(options){
 if(!options||typeof options!=="object"||Array.isArray(options))throw new TypeError("options must be an object");
 const baseUrl=origin(options.baseUrl),serviceToken=nonEmpty(options.serviceToken,"serviceToken"),serverId=uuid(options.serverId,"serverId");
 const timeoutMs=options.timeoutMs??DEFAULT_TIMEOUT_MS;bounded(timeoutMs,"timeoutMs",100,MAX_TIMEOUT_MS);
 const fetchImpl=options.fetchImpl??globalThis.fetch;if(typeof fetchImpl!=="function")throw new TypeError("fetchImpl must be a function");
 async function request(path,body){const response=await fetchImpl(baseUrl+path,{method:"POST",headers:{accept:"application/json","content-type":"application/json",authorization:"Bearer "+serviceToken},body:JSON.stringify(body),cache:"no-store",redirect:"error",signal:AbortSignal.timeout(timeoutMs)});return readData(response);}
 async function claim(claimOptions={}){
  if(!claimOptions||typeof claimOptions!=="object"||Array.isArray(claimOptions))throw new TypeError("claim options must be an object");
  const limit=claimOptions.limit??25,leaseSeconds=claimOptions.leaseSeconds??120;bounded(limit,"limit",1,MAX_CLAIM_LIMIT);bounded(leaseSeconds,"leaseSeconds",MIN_LEASE_SECONDS,MAX_LEASE_SECONDS);
  const path="/integrations/servers/"+encodeURIComponent(serverId)+"/events/claim";
  const data=await request(path,{limit,leaseSeconds,eventTypes:TRIBE_OUTBOX_EVENT_TYPES});if(!Array.isArray(data))throw new TypeError("Core claim response data must be an array");
  return Object.freeze(data.map((row,index)=>{
   if(!row||typeof row!=="object"||Array.isArray(row))throw new TypeError("Core claim response data["+index+"] must be an object");uuid(row.id,"claimed event id");uuid(row.leaseToken,"claimed event leaseToken");
   if(row.eventType!=="game.event.received"||!row.payload||typeof row.payload!=="object"||Array.isArray(row.payload)||row.payload.serverId!==serverId)throw new TypeError("Core claim response contains an event outside the requested server scope");
   if(!ALLOWED_TYPES.has(row.payload.eventType))throw new TypeError("Core claim response contains an unsupported tribe event type");
   const normalized=normalizeCoreTribeEvent({eventType:row.eventType,payload:row.payload});return Object.freeze({...normalized,outboxEvent:Object.freeze({...row})});
  }));
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
