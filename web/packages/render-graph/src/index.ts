import type { DrawStats, Hit, Renderer, Viewport } from "@datafocal/render-core";
import { type DrawList, drawList, edgeStride, type GraphModel, hitGrid, nodeStride } from "./lod";

export type { GraphModel } from "./lod";

// GraphRenderer labels the focus and its neighbours however small they are drawn.
export interface GraphRenderer extends Renderer<GraphModel> {
  focus(index: number | null): void;
}

const header = `#version 300 es
precision mediump float;
uniform vec2 u_size;
vec4 clip(vec2 p) { return vec4(p / u_size * vec2(2, -2) + vec2(-1, 1), 0, 1); }
`;

// Each instance is a quad drawn as a four-vertex strip, its corner from gl_VertexID.
const corner = "vec2(gl_VertexID & 1, gl_VertexID >> 1)";

const nodeVertex = `${header}
layout(location = 0) in vec4 a_node;
layout(location = 1) in float a_kind;
out vec2 v_p;
flat out vec2 v_style;
void main() {
  v_p = ${corner} * 2.0 - 1.0;
  v_style = vec2(a_node.w, a_kind);
  gl_Position = clip(a_node.xy + v_p * a_node.z);
}`;

const nodeFragment = `${header}
in vec2 v_p;
flat in vec2 v_style;
out vec4 color;
const vec4 health[4] = vec4[](vec4(.30, .69, .47, 1), vec4(.93, .65, .20, 1),
  vec4(.89, .29, .27, 1), vec4(.55, .60, .68, .3));
void main() {
  if (v_style.y < 3.5 && dot(v_p, v_p) > 1.0) discard;
  color = health[int(v_style.x)];
}`;

const edgeVertex = `${header}
layout(location = 0) in vec4 a_line;
layout(location = 1) in vec4 a_style;
out vec2 v_p;
flat out vec4 v_style;
void main() {
  vec2 c = ${corner};
  vec2 d = a_line.zw - a_line.xy;
  float len = length(d);
  vec2 n = len > 0.0 ? vec2(-d.y, d.x) / len : vec2(0, 1);
  float half_ = a_style.x * (a_style.z == 3.0 ? 1.5 : 0.5) + 0.5;
  v_p = vec2(c.x * len, c.y * 2.0 - 1.0);
  v_style = a_style;
  gl_Position = clip(a_line.xy + d * c.x + n * v_p.y * half_);
}`;

// Line style is the inference method: solid, long-dash, dotted or
// double. Opacity is confidence; ghost edges are grey and faint, async ones violet.
const edgeFragment = `${header}
in vec2 v_p;
flat in vec4 v_style;
out vec4 color;
void main() {
  int method = int(v_style.z);
  int flags = int(v_style.w);
  if (method == 1 && mod(v_p.x, 12.0) > 8.0) discard;
  if (method == 2 && mod(v_p.x, 4.0) > 1.5) discard;
  if (method == 3 && abs(v_p.y) < 0.34) discard;
  color = vec4((flags & 2) != 0 ? vec3(.55, .45, .85) : vec3(.55, .6, .68), v_style.y);
  if ((flags & 1) != 0) color = vec4(.6, .6, .6, v_style.y * .35);
}`;

const labelVertex = `${header}
uniform vec2 u_atlas;
layout(location = 0) in vec4 a_box;
layout(location = 1) in vec4 a_uv;
out vec2 v_uv;
void main() {
  vec2 c = ${corner};
  v_uv = (a_uv.xy + c * a_uv.zw) / u_atlas;
  gl_Position = clip(a_box.xy + c * a_box.zw);
}`;

const labelFragment = `${header}
uniform sampler2D u_text;
in vec2 v_uv;
out vec4 color;
void main() { color = texture(u_text, v_uv); }`;

const atlasSize = 1024;
const fontSize = 13;

// Theme is the colours the renderer takes from the page: a canvas in a worker can't read
// the page's CSS.
export interface Theme {
  readonly label: string;
}

interface Pass {
  program: WebGLProgram;
  vao: WebGLVertexArrayObject;
  buffer: WebGLBuffer;
}

class Graph implements GraphRenderer {
  private readonly gl: WebGL2RenderingContext;
  private readonly nodes: Pass;
  private readonly edges: Pass;
  private readonly labels: Pass;
  private readonly canvas: OffscreenCanvas;
  private readonly atlas: OffscreenCanvasRenderingContext2D;
  private readonly text: WebGLTexture;
  // placed is where each model node's label is in the atlas, packed in rows from shelf.
  private placed = new Map<number, [number, number, number, number]>();
  private shelf = { x: 0, y: 0 };
  private hit = (_x: number, _y: number) => -1;
  private focused: number | null = null;
  private last: Viewport | null = null;
  private size = { width: 1, height: 1, dpr: 1 };

  private readonly theme: Theme;

  constructor(canvas: OffscreenCanvas, theme: Theme) {
    this.theme = theme;
    const gl = canvas.getContext("webgl2", { premultipliedAlpha: false });
    const atlas = new OffscreenCanvas(atlasSize, atlasSize).getContext("2d");
    if (!gl || !atlas) {
      throw new Error("render-graph needs WebGL2 and a 2D canvas");
    }
    this.canvas = canvas;
    this.gl = gl;
    this.atlas = atlas;
    this.nodes = this.pass(nodeVertex, nodeFragment, [4, 1]);
    this.edges = this.pass(edgeVertex, edgeFragment, [4, 4]);
    this.labels = this.pass(labelVertex, labelFragment, [4, 4]);
    this.text = gl.createTexture();
    gl.bindTexture(gl.TEXTURE_2D, this.text);
    gl.texStorage2D(gl.TEXTURE_2D, 1, gl.RGBA8, atlasSize, atlasSize);
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.LINEAR);
    gl.enable(gl.BLEND);
    // Colour blends by the source's alpha; alpha itself adds, so text over the clear canvas
    // keeps its alpha rather than its square.
    gl.blendFuncSeparate(gl.SRC_ALPHA, gl.ONE_MINUS_SRC_ALPHA, gl.ONE, gl.ONE_MINUS_SRC_ALPHA);
  }

  resize(width: number, height: number, dpr: number): void {
    this.size = { width, height, dpr };
    this.canvas.width = Math.round(width * dpr);
    this.canvas.height = Math.round(height * dpr);
    this.gl.viewport(0, 0, this.canvas.width, this.canvas.height);
    this.clearAtlas();
  }

  render(model: GraphModel, view: Viewport): DrawStats {
    const last = this.last;
    const panning =
      last !== null &&
      last.x1 - last.x0 === view.x1 - view.x0 &&
      last.y1 - last.y0 === view.y1 - view.y0 &&
      (last.x0 !== view.x0 || last.y0 !== view.y0);
    this.last = view;
    const list = drawList(model, view, this.focused, panning);
    this.hit = hitGrid(list, view.width, view.height);
    const gl = this.gl;
    gl.clear(gl.COLOR_BUFFER_BIT);
    const calls = [
      this.draw(this.edges, list.edges, edgeStride),
      this.draw(this.nodes, list.nodes, nodeStride),
      this.draw(this.labels, this.labelBoxes(model, list), 8),
      this.draw(this.nodes, list.minimap.nodes, nodeStride),
      this.draw(this.edges, list.minimap.edges, edgeStride),
    ];
    return {
      drawCalls: calls.filter(Boolean).length,
      primitives: list.index.length,
      labels: list.labels.length,
    };
  }

  hitTest(x: number, y: number): Hit | null {
    const index = this.hit(x, y);
    return index < 0 ? null : { kind: "node", index };
  }

  focus(index: number | null): void {
    this.focused = index;
  }

  dispose(): void {
    this.gl.getExtension("WEBGL_lose_context")?.loseContext();
  }

  private pass(vertex: string, fragment: string, sizes: number[]): Pass {
    const gl = this.gl;
    const program = gl.createProgram();
    gl.attachShader(program, this.shader(gl.VERTEX_SHADER, vertex));
    gl.attachShader(program, this.shader(gl.FRAGMENT_SHADER, fragment));
    gl.linkProgram(program);
    if (!gl.getProgramParameter(program, gl.LINK_STATUS)) {
      throw new Error(`render-graph: ${gl.getProgramInfoLog(program) ?? ""}`);
    }
    const vao = gl.createVertexArray();
    const buffer = gl.createBuffer();
    gl.bindVertexArray(vao);
    gl.bindBuffer(gl.ARRAY_BUFFER, buffer);
    const stride = sizes.reduce((a, b) => a + b) * 4;
    sizes.reduce((offset, size, location) => {
      gl.enableVertexAttribArray(location);
      gl.vertexAttribPointer(location, size, gl.FLOAT, false, stride, offset);
      gl.vertexAttribDivisor(location, 1);
      return offset + size * 4;
    }, 0);
    return { program, vao, buffer };
  }

  private shader(type: GLenum, source: string): WebGLShader {
    const shader = this.gl.createShader(type);
    if (!shader) {
      throw new Error("render-graph cannot create a shader");
    }
    this.gl.shaderSource(shader, source);
    this.gl.compileShader(shader);
    return shader;
  }

  // draw draws the instances in data and reports whether it made a draw call.
  private draw(pass: Pass, data: Float32Array, stride: number): boolean {
    const gl = this.gl;
    if (data.length === 0) {
      return false;
    }
    gl.useProgram(pass.program);
    gl.uniform2f(gl.getUniformLocation(pass.program, "u_size"), this.size.width, this.size.height);
    gl.uniform2f(gl.getUniformLocation(pass.program, "u_atlas"), atlasSize, atlasSize);
    gl.bindVertexArray(pass.vao);
    gl.bindBuffer(gl.ARRAY_BUFFER, pass.buffer);
    gl.bufferData(gl.ARRAY_BUFFER, data, gl.STREAM_DRAW);
    gl.drawArraysInstanced(gl.TRIANGLE_STRIP, 0, 4, data.length / stride);
    return true;
  }

  // labelBoxes places each label centred under its node, clear of the edges that leave it
  // sideways, as a box on the canvas and its text's rectangle in the atlas.
  private labelBoxes(model: GraphModel, list: DrawList): Float32Array {
    const out = new Float32Array(list.labels.length * 8);
    list.labels.forEach((s, k) => {
      const [x, y, r] = list.nodes.subarray(s * nodeStride, s * nodeStride + 3);
      const [u, v, w, h] = this.place(model, list.index[s] ?? 0);
      const dpr = this.size.dpr;
      out.set(
        [(x ?? 0) - w / dpr / 2, (y ?? 0) + (r ?? 0) + 2, w / dpr, h / dpr, u, v, w, h],
        k * 8,
      );
    });
    return out;
  }

  // place returns where node i's label is in the atlas, drawing it there first if need be.
  // A full atlas starts over, so it holds the labels of the frames since.
  private place(model: GraphModel, i: number): [number, number, number, number] {
    const ctx = this.atlas;
    const found = this.placed.get(i);
    if (found) {
      return found;
    }
    const members = model.nodes.members[i] ?? 0;
    const text = `${model.labels[i] ?? ""}${members > 1 ? ` (${members})` : ""}`;
    ctx.font = `${fontSize * this.size.dpr}px sans-serif`;
    const [w, h] = [
      Math.ceil(ctx.measureText(text).width),
      Math.ceil(fontSize * 1.4 * this.size.dpr),
    ];
    if (this.shelf.x + w > atlasSize) {
      this.shelf = { x: 0, y: this.shelf.y + h };
    }
    if (this.shelf.y + h > atlasSize) {
      this.clearAtlas();
    }
    const box: [number, number, number, number] = [
      this.shelf.x,
      this.shelf.y,
      Math.min(w, atlasSize),
      h,
    ];
    ctx.clearRect(...box);
    ctx.fillStyle = this.theme.label;
    ctx.textBaseline = "middle";
    ctx.fillText(text, box[0], box[1] + h / 2);
    const gl = this.gl;
    gl.bindTexture(gl.TEXTURE_2D, this.text);
    gl.texSubImage2D(
      gl.TEXTURE_2D,
      0,
      box[0],
      box[1],
      gl.RGBA,
      gl.UNSIGNED_BYTE,
      ctx.getImageData(...box),
    );
    this.shelf.x += w;
    this.placed.set(i, box);
    return box;
  }

  private clearAtlas(): void {
    this.placed.clear();
    this.shelf = { x: 0, y: 0 };
    this.atlas.clearRect(0, 0, atlasSize, atlasSize);
  }
}

// createGraphRenderer draws service graphs into canvas, which may live in a worker, its
// labels in theme's colour.
export function createGraphRenderer(
  canvas: OffscreenCanvas,
  theme: Theme = { label: "#aab1bf" },
): GraphRenderer {
  return new Graph(canvas, theme);
}
