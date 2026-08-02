import type { Component } from "vue"

/** One selectable option. `key` is what the caller binds; `label` is what reka matches on. */
export type ComboboxItemOption = {
  key: string
  label: string
  icon?: Component
  iconUrl?: string
  initials?: string
  disabled?: boolean
}
