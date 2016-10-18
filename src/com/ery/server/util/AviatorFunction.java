package com.ery.server.util;

import java.util.Map;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.runtime.function.AbstractFunction;
import com.googlecode.aviator.runtime.function.FunctionUtils;
import com.googlecode.aviator.runtime.type.AviatorObject;
import com.googlecode.aviator.runtime.type.AviatorString;

public class AviatorFunction {
	static int funSize = 0;

	public static int getFunSize() {
		return funSize;
	}

	public synchronized static void addFunction(AbstractFunction fun) {
		AviatorEvaluator.addFunction(new ReverseString());
		funSize++;
	}

	public static class ReverseString extends AbstractFunction {
		static {
			AviatorFunction.addFunction(new ReverseString());
		}

		public AviatorObject call(Map env, AviatorObject arg1) {
			String left = FunctionUtils.getStringValue(arg1, env);
			StringBuffer mstr = new StringBuffer(left);
			return new AviatorString(mstr.reverse().toString());
		}

		public AviatorObject call(Map env, AviatorObject arg1, AviatorObject arg2) {
			String left = FunctionUtils.getStringValue(arg1, env);
			String left2 = FunctionUtils.getStringValue(arg2, env);
			StringBuffer mstr = new StringBuffer(left);
			mstr = mstr.reverse();
			StringBuffer mstr2 = new StringBuffer(left2);
			mstr.append(mstr2.reverse());
			return new AviatorString(mstr.toString());
		}

		@Override
		public String getName() {
			return "reverse";
		}
	}

}
