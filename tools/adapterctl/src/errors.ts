export class CliError extends Error {
  readonly exitCode: number;

  constructor(message: string, exitCode = 1) {
    super(message);
    this.exitCode = exitCode;
  }
}

export class ValidationError extends CliError {
  readonly errors: string[];

  constructor(errors: string[]) {
    super(errors.join("\n"));
    this.errors = errors;
  }
}
