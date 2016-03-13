/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package controllers;

import org.junit.Test;
import play.api.mvc.Content;
import play.test.WithApplication;
import views.html.page.homePage;
import views.html.results.searchResults;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class ApplicationTest extends WithApplication {

  @Test
  public void testRenderHomePage() {
    Content html = homePage.render(5, 2, 3, searchResults.render("Latest analysis", null));
    assertEquals("text/html", html.contentType());
    assertTrue(html.body().contains("Hello there, I've been busy!"));
    assertTrue(html.body().contains("I looked through <b>5</b> jobs today."));
    assertTrue(html.body().contains("About <b>2</b> of them could use some tuning."));
    assertTrue(html.body().contains("About <b>3</b> of them need some serious attention!"));
  }

  @Test
  public void testRenderSearch() {
    Content html = searchResults.render("Latest analysis", null);
    assertEquals("text/html", html.contentType());
    assertTrue(html.body().contains("Latest analysis"));
  }

}
