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

package org.apache.hadoop.fs.shell;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathExistsException;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.PathNotFoundException;

import java.io.IOException;
import java.util.LinkedList;

/**
 * Symbolic links related operations
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable

public class Ln extends FsCommand {

  public static void registerCommands(CommandFactory factory) {
    factory.addClass(Ln.class, "-ln");
  }

  private static final String OPTION_SYMBOLIC = "s";

  public static final String NAME = "ln";
  public static final String USAGE = "[-" + OPTION_SYMBOLIC + "] <target_path> <link_path>";
  public static final String DESCRIPTION =
    "Create a symlink from the <link_path> to <target_path>.\n" +
    "Symbolic link targets are stored as-is and can be\n" +
    "relative, absolute, or fully-qualified Paths.\n" +
    "\n" +
    "Currently, this command only supports creation of\n" +
    "symbolic links.\n" +
    "\n" +
    "-" + OPTION_SYMBOLIC + "Creates a symbolic link.\n";

  private boolean symbolic = false;

  @Override
  protected void processOptions(LinkedList<String> args)
      throws IOException {
    CommandFormat cf = new CommandFormat(2, 2, "s");
    cf.parse(args);
    symbolic = cf.getOpt(OPTION_SYMBOLIC);
  }

  @Override
  protected void processArguments(LinkedList<PathData> args) throws IOException {
    if (!symbolic) {
      throw new IOException("Hardlinks are not supported. " +
          "Use -s to create a symbolic link.");
    }

    PathData target = args.get(0);
    PathData link = args.get(1);
    String targetUri = target.fs.getUri().getScheme() + "://" + target.fs.getUri().getHost();
    String linkUri = link.fs.getUri().getScheme() + "://" + link.fs.getUri().getHost();

    if (!targetUri.equals(linkUri)) {
      throw new PathIOException(target.toString(), "Does not match target filesystem");
    }

    if (!target.exists) {
      throw new PathNotFoundException(target.toString());
    }

    if (link.exists) {
      throw new PathExistsException(link.toString());
    }

    Path targetPath = target.path;
    Path linkPath = link.path;
    link.fs.createSymlink(targetPath, linkPath, false);
  }

}
