import { computed, type Ref } from "vue";
import { useInfiniteQuery, type InfiniteData } from "@tanstack/vue-query";

import { getHistory } from "@/api";
import { HISTORY_PAGE_SIZE, HISTORY_QUERY_KEY } from "@/ui";
import type { HistoryResponse, TaskItem } from "@/types";
import { errorMessage } from "@/utils/dashboard";

interface UseHistoryOptions {
  sourceKey: Ref<string>;
  search: Ref<string>;
  enabled: Ref<boolean>;
}

// Paginated history: one page per fetch, appended on scroll, so the browser never holds the whole table.
export function useHistory({ sourceKey, search, enabled }: UseHistoryOptions) {
  const searchQuery = computed(() => search.value.trim());
  const query = useInfiniteQuery<
    HistoryResponse,
    Error,
    InfiniteData<HistoryResponse, string | undefined>,
    readonly unknown[],
    string | undefined
  >({
    queryKey: [...HISTORY_QUERY_KEY, HISTORY_PAGE_SIZE, sourceKey, searchQuery],
    queryFn: ({ pageParam }) => getHistory(pageParam, HISTORY_PAGE_SIZE, sourceKey.value, searchQuery.value),
    initialPageParam: undefined,
    getNextPageParam: (lastPage) => lastPage.next_cursor ?? undefined,
    enabled,
    staleTime: 1000,
  });

  const entries = computed<TaskItem[]>(() => (query.data.value?.pages || []).flatMap((page) => page.entries));
  const loading = computed(() => query.isLoading.value);
  const fetchingMore = computed(() => query.isFetchingNextPage.value);
  const hasMore = computed(() => Boolean(query.hasNextPage.value));
  const historyError = computed(() =>
    query.error.value ? errorMessage(query.error.value, "Could not load history.") : "",
  );

  function loadMore(): void {
    if (query.hasNextPage.value && !query.isFetchingNextPage.value) void query.fetchNextPage();
  }

  async function refresh(): Promise<void> {
    await query.refetch();
  }

  return { entries, loading, fetchingMore, hasMore, historyError, loadMore, refresh };
}
