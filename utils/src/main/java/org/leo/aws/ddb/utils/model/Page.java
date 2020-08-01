package org.leo.aws.ddb.utils.model;

import java.util.Objects;

@SuppressWarnings({"unused"})

public class Page {

    private int size;

    private int totalElements;

    private int totalPages;

    private int currentPage;

    public Page() {}

    public Page(int size, int totalElements, int totalPages, int currentPage) {
        this.size = size;
        this.totalElements = totalElements;
        this.totalPages = totalPages;
        this.currentPage = currentPage;
    }

    public int getSize() {
        return size;
    }

    public int getTotalElements() {
        return totalElements;
    }

    public int getTotalPages() {
        return totalPages;
    }

    public int getCurrentPage() {
        return currentPage;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Page page = (Page) o;
        return size == page.size &&
               totalElements == page.totalElements &&
               totalPages == page.totalPages &&
               currentPage == page.currentPage;
    }

    public static PageBuilder builder() {
        return new PageBuilder();
    }

    @Override
    public int hashCode() {
        return Objects.hash(size, totalElements, totalPages, currentPage);
    }

    @Override
    public String toString() {
        return "Page{" +
               "size=" + size +
               ", totalElements=" + totalElements +
               ", totalPages=" + totalPages +
               ", currentPage=" + currentPage +
               '}';
    }

    public static class PageBuilder {
        private int size;
        private int totalElements;
        private int totalPages;
        private int currentPage;

        PageBuilder() {}

        public PageBuilder size(int size) {
            this.size = size;
            return this;
        }

        public PageBuilder totalElements(int totalElements) {
            this.totalElements = totalElements;
            return this;
        }

        public PageBuilder totalPages(int totalPages) {
            this.totalPages = totalPages;
            return this;
        }

        public PageBuilder currentPage(int currentPage) {
            this.currentPage = currentPage;
            return this;
        }

        public Page build() {
            return new Page(size, totalElements, totalPages, currentPage);
        }

        @Override
        public String toString() {
            return "PageBuilder{" +
                   "size=" + size +
                   ", totalElements=" + totalElements +
                   ", totalPages=" + totalPages +
                   ", currentPage=" + currentPage +
                   '}';
        }
    }
}
