// Payload schemas. Any library that implements Standard Schema
// (https://standardschema.dev) works: zod, valibot, arktype, ...
// The interface is copied from `@standard-schema/spec` v1, as the spec
// allows, so the SDK has no runtime dependency on it.

/** A schema in the Standard Schema v1 format. */
export interface StandardSchemaV1<Input = unknown, Output = Input> {
  readonly "~standard": StandardSchemaV1.Props<Input, Output>;
}

export declare namespace StandardSchemaV1 {
  export interface Props<Input = unknown, Output = Input> {
    readonly version: 1;
    readonly vendor: string;
    readonly validate: (value: unknown) => Result<Output> | Promise<Result<Output>>;
    readonly types?: Types<Input, Output> | undefined;
  }

  export type Result<Output> = SuccessResult<Output> | FailureResult;

  export interface SuccessResult<Output> {
    readonly value: Output;
    readonly issues?: undefined;
  }

  export interface FailureResult {
    readonly issues: ReadonlyArray<Issue>;
  }

  export interface Issue {
    readonly message: string;
    readonly path?: ReadonlyArray<PropertyKey | PathSegment> | undefined;
  }

  export interface PathSegment {
    readonly key: PropertyKey;
  }

  export interface Types<Input = unknown, Output = Input> {
    readonly input: Input;
    readonly output: Output;
  }

  export type InferInput<Schema extends StandardSchemaV1> = NonNullable<Schema["~standard"]["types"]>["input"];
  export type InferOutput<Schema extends StandardSchemaV1> = NonNullable<Schema["~standard"]["types"]>["output"];
}

/** How validating `value` against a schema went. */
export type Validated<T> = { ok: true; value: T } | { ok: false; message: string } | { ok: "async" };

/** Validate synchronously: replay cannot wait on a schema. */
export function validate<T>(schema: StandardSchemaV1<unknown, T>, value: unknown): Validated<T> {
  const result = schema["~standard"].validate(value);
  if (result instanceof Promise) {
    result.catch(() => {}); // never awaited
    return { ok: "async" };
  }
  if (result.issues) return { ok: false, message: result.issues.map(describe).join("; ") };
  return { ok: true, value: result.value };
}

function describe(issue: StandardSchemaV1.Issue): string {
  const path = (issue.path ?? []).map((s) => String(typeof s === "object" ? s.key : s)).join(".");
  return path ? `${path}: ${issue.message}` : issue.message;
}
