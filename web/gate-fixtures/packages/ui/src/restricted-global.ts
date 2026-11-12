export function load(): Promise<Response> {
  return fetch("/v1/describe");
}
