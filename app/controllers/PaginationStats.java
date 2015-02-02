package controllers;

public class PaginationStats {
  public int currentPage = 1;
  public int paginationBarStartIndex = 1;
  public int paginationBarEndIndex = 1;
  public int pageLength;
  public int pageBarLength;
  public String queryString = null;

  public PaginationStats(int pageLength, int pageBarLength) {
    this.pageLength = pageLength;
    this.pageBarLength = pageBarLength;
  }

  public int getCurrentPage() {
    return currentPage;
  }

  public void setCurrentPage(int currentPage) {
    if (currentPage < 1) {
      this.currentPage = 1;
    } else {
      this.currentPage = currentPage;
    }
  }

  /**
   * Compute paginationBarStartIndex.
   * paginationBarStartIndex is computed such that the currentPage remains at the center of the Pagination Bar.
   */
  public int getPaginationBarStartIndex() {
    this.paginationBarStartIndex = Math.max(this.currentPage - this.pageBarLength / 2, 1);
    return this.paginationBarStartIndex;
  }

  public int computePaginationBarEndIndex(int resultSize) {
    this.paginationBarEndIndex = this.paginationBarStartIndex + (resultSize - 1) / this.pageLength;
    return this.paginationBarEndIndex;
  }

  public int getPaginationBarEndIndex() {
    return this.paginationBarEndIndex;
  }

  public String getQueryString() {
    return queryString;
  }

  public void setQueryString(String queryString) {
    this.queryString = queryString;
  }

  public int getPageBarLength() {
    return pageBarLength;
  }

  public int getPageLength() {
    return pageLength;
  }
}
