export class CliError extends Error {
  readonly exitCode: number;

  constructor(args: { message: string; exitCode?: number }) {
    super(args.message);
    this.exitCode = args.exitCode ?? 1;
  }
}

export class ValidationError extends CliError {
  readonly errors: string[];

  constructor(errors: string[]) {
    super({ message: errors.join("\n") });
    this.errors = errors;
  }
}
