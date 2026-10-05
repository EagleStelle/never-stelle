import { reactive } from "vue";

type FieldValue = number | boolean;

interface InheritedFieldsSource<F extends string> {
  // Overrides by key, read through the draft's reactive proxy.
  entries: () => Record<string, Partial<Record<F, FieldValue>>>;
  // The value a field shows while it has no override.
  inherited: (field: F) => FieldValue;
}

// Fields that start filled with the value they inherit; clearing a field or setting
// that value back drops the override so it keeps following its default.
export function useInheritedFields<F extends string>({
  entries,
  inherited,
}: InheritedFieldsSource<F>) {
  // Text being typed, so a cleared field stays empty until it loses focus.
  const edits = reactive<Record<string, string>>({});

  function editKey(key: string, field: F): string {
    return `${key}:${field}`;
  }

  function value(key: string, field: F): FieldValue {
    return entries()[key]?.[field] ?? inherited(field);
  }

  function fieldValue(key: string, field: F): string {
    const editing = edits[editKey(key, field)];
    if (editing !== undefined) return editing;
    return String(value(key, field));
  }

  function setValue(key: string, field: F, next: FieldValue): void {
    const record = entries();
    if (next === inherited(field)) {
      delete record[key]?.[field];
    } else {
      record[key] ??= {};
      record[key][field] = next;
    }
  }

  function setField(key: string, field: F, raw: string | number): void {
    const text = String(raw ?? "").trim();
    edits[editKey(key, field)] = text;
    const parsed = Number(text);
    if (!text) {
      delete entries()[key]?.[field];
    } else if (Number.isFinite(parsed)) {
      setValue(key, field, parsed);
    }
  }

  function endEdit(key: string, field: F): void {
    delete edits[editKey(key, field)];
  }

  return { value, fieldValue, setValue, setField, endEdit };
}
