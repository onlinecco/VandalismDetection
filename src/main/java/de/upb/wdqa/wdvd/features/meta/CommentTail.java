package de.upb.wdqa.wdvd.features.meta;

import de.upb.wdqa.wdvd.Revision;
import de.upb.wdqa.wdvd.features.FeatureImpl;
import de.upb.wdqa.wdvd.features.FeatureStringValue;

public class CommentTail extends FeatureImpl {

	@Override
	public FeatureStringValue calculate(Revision revision) {
		String result = revision.getParsedComment().getSuffixComment();
		
		if (result!= null){
			result = result.trim();
		}
		
		return new FeatureStringValue(result);
	}

}
