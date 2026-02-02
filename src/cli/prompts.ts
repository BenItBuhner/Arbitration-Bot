import prompts from "prompts";

export type PromptChoice<T extends string = string> = {
  title: string;
  value: T;
  description?: string;
  disabled?: boolean;
};

type PromptQuestion = {
  type: string;
  name: string;
  message: string;
  choices?: PromptChoice<string>[];
  initial?: unknown;
  validate?: (value: string) => true | string;
  min?: number;
};

async function runPrompt<T>(question: PromptQuestion): Promise<T | null> {
  let cancelled = false;
  const response = await prompts(question, {
    onCancel: () => {
      cancelled = true;
      return true;
    },
  });
  if (cancelled) return null;
  return (response as { value?: T }).value ?? null;
}

export async function selectOne<T extends string>(
  message: string,
  choices: PromptChoice<T>[],
  initial: number = 0,
): Promise<T | null> {
  if (choices.length === 0) return null;
  const normalized = choices.map((choice) => ({
    title: choice.title,
    value: choice.value,
    description: choice.description,
    disabled: choice.disabled,
  }));
  return runPrompt<T>({
    type: "select",
    name: "value",
    message,
    choices: normalized,
    initial,
  });
}

export async function selectMany<T extends string>(
  message: string,
  choices: PromptChoice<T>[],
  options: { min?: number; initial?: number[] } = {},
): Promise<T[]> {
  if (choices.length === 0) return [];
  const normalized = choices.map((choice) => ({
    title: choice.title,
    value: choice.value,
    description: choice.description,
    disabled: choice.disabled,
  }));
  const result = await runPrompt<T[]>({
    type: "multiselect",
    name: "value",
    message,
    choices: normalized,
    min: options.min ?? 1,
    initial: options.initial,
  });
  return result ?? [];
}

export async function promptText(
  message: string,
  options: { initial?: string; validate?: (value: string) => true | string } = {},
): Promise<string | null> {
  const result = await runPrompt<string>({
    type: "text",
    name: "value",
    message,
    initial: options.initial,
    validate: options.validate,
  });
  if (result === null) return null;
  return result.trim();
}

export async function promptConfirm(
  message: string,
  initial: boolean = false,
): Promise<boolean | null> {
  return runPrompt<boolean>({
    type: "confirm",
    name: "value",
    message,
    initial,
  });
}
