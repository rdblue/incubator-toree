/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License
 */

package org.apache.toree.kernel.protocol.v5.magic

import org.apache.toree.interpreter.Interpreter
import org.apache.toree.kernel.protocol.v5._
import org.apache.toree.magic.{CellMagicOutput, LineMagicOutput}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSpec, Matchers}

class PostProcessorSpec extends FunSpec with Matchers with MockitoSugar{
  describe("#matchCellMagic") {
    it("should return the cell magic output when the Left contains a " +
       "CellMagicOutput") {
      val processor = new PostProcessor(mock[Interpreter])
      val codeOutput = "some output"
      val cmo = CellMagicOutput()
      val left = Left(cmo)
      processor.matchCellMagic(codeOutput, left) should be(cmo)
    }

    it("should package the original code when the Left does not contain a " +
       "CellMagicOutput") {
      val processor = new PostProcessor(mock[Interpreter])
      val codeOutput = "some output"
      val left = Left("")
      val data = Data(MIMEType.PlainText -> codeOutput)
      processor.matchCellMagic(codeOutput, left) should be(data)
    }
  }

  describe("#matchLineMagic") {
    it("should process the code output when the Right contains a " +
       "LineMagicOutput") {
      val processor = spy(new PostProcessor(mock[Interpreter]))
      val codeOutput = "some output"
      val lmo = LineMagicOutput
      val right = Right(lmo)
      processor.matchLineMagic(codeOutput, right)
      verify(processor).processLineMagic(codeOutput)
    }

    it("should package the original code when the Right does not contain a " +
       "LineMagicOutput") {
      val processor = new PostProcessor(mock[Interpreter])
      val codeOutput = "some output"
      val right = Right("")
      val data = Data(MIMEType.PlainText -> codeOutput)
      processor.matchLineMagic(codeOutput, right) should be(data)
    }
  }

  describe("#processLineMagic") {
    it("should remove the result of the magic invocation if it is the last " +
       "line") {
      val processor = new PostProcessor(mock[Interpreter])
      val x = "hello world"
      val codeOutput = s"$x\nsome other output"
      val data = Data(MIMEType.PlainText -> x)
      processor.processLineMagic(codeOutput) should be(data)
    }
  }
}
