import { computed, type Ref } from "vue";
import { useInfiniteQuery } from "@tanstack/vue-query";

import { getHistory } from "../api";
import { HISTORY_PAGE_SIZE, HISTORY_QUERY_KEY } from "../ui";
import type { HistoryResponse, TaskItem } from "../types";
import { errorMessage } from "../utils/dashboard";

interface UseHistoryOptions {
  sourceKey: Ref<string>;
  search: Ref<string>;
  enabled: Ref<boolean>;
}

// Paginated history: one page per fetch, appended on scroll, so the browser never holds the whole table.
export function useHistory({ sourceKey, search, enabled }: UseHistoryOptions) {
  const query = useInfiniteQuery<HistoryResponse>({
    queryKey: [...HISTORY_QUERY_KEY, sourceKey, search],
    queryFn: ({ pageParam }) => getHistory(pageParam as number, HISTORY_PAGE_SIZE, sourceKey.value, search.value),
    initialPageParam: 0,
    getNextPageParam: (lastPage, allPages) => {
      const loaded = allPages.reduce((sum, page) => sum + page.entries.length, 0);
      return loaded < lastPage.total ? loaded : undefined;
    },
    enabled,
    staleTime: 1000,
  });

  const entries = computed<TaskItem[]>(() => (query.data.value?.pages || []).flatMap((page) => page.entries));
  const total = computed(() => query.data.value?.pages?.[0]?.total ?? 0);
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

  return { entries, total, loading, fetchingMore, hasMore, historyError, loadMore, refresh };
}
