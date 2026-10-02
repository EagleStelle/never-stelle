// Plain HTTP has no clipboard API, so a hidden field's selection stands in there. It
// goes inside `host`, since a dialog's focus trap pulls focus back from one outside it.
export async function copyText(text: string, host: HTMLElement): Promise<boolean> {
  try {
    await navigator.clipboard.writeText(text);
    return true;
  } catch {
    const area = document.createElement("textarea");
    area.value = text;
    area.readOnly = true;
    area.className = "fixed top-0 left-0 opacity-0";
    host.append(area);
    area.select();
    const copied = document.execCommand("copy");
    area.remove();
    return copied;
  }
}
