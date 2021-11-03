/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.hadoop.yarn.server.globalpolicygenerator.webapp;

import com.google.inject.Inject;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GlobalPolicyGenerator;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

/**
 * GPGRouterList block for the GPG Web UI.
 */
public class GPGRouterListBlock extends HtmlBlock {

  private final GlobalPolicyGenerator gpg;

  @Inject
  GPGRouterListBlock(GlobalPolicyGenerator gpg, ViewContext ctx) {
    super(ctx);
    this.gpg = gpg;
  }

  @Override
  protected void render(Block html) {
    String manageRouterGroups= gpg.getManageRouterList();
    String[] manageRouterGroupArray = manageRouterGroups.split(",");

    for(String groupName : manageRouterGroupArray) {
      // Table header
      Hamlet.TBODY<Hamlet.TABLE<Hamlet>> tbody = html.table().thead().tr().
          th("Group: " + groupName).__().__().tbody();
      String[] groupRouters = gpg.getMachineListByRouterGroup(groupName).
          split(",");
      int i = 0;
      for (String singleRouter : groupRouters) {
        // Building row per router server
        tbody.tr().td().a(singleRouter,"Router_" + i).__().
            td(singleRouter).__();
        i++;
      }
      tbody.__().__().div().p().__("******************************").__().__();
    }
  }

}
