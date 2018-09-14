package org.neo4j.gis.osm.model;

import org.junit.Test;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Values;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class OSMModelTest {

    @Test
    public void shouldCalculateAnglesInTriangle() {
        PointValue[] left = new PointValue[]{
                Values.pointValue(CoordinateReferenceSystem.Cartesian, 0, 0),
                Values.pointValue(CoordinateReferenceSystem.Cartesian, 0, 0)
        };
        PointValue[] right = new PointValue[]{
                Values.pointValue(CoordinateReferenceSystem.Cartesian, 10, 0),
                Values.pointValue(CoordinateReferenceSystem.Cartesian, 10, -10)
        };
        for (int x = 10; x >= -10; x--) {
            PointValue[] apex = new PointValue[]{
                    Values.pointValue(CoordinateReferenceSystem.Cartesian, x, 10),
                    Values.pointValue(CoordinateReferenceSystem.Cartesian, 10 + x, 10 - x)
            };
            double[] leftAngle = new double[apex.length];
            double[] rightAngle = new double[apex.length];
            double[] apexAngle = new double[apex.length];
            OSMModel.Triangle[] triangle = new OSMModel.Triangle[apex.length];
            for (int i = 0; i < apex.length; i++) {
                triangle[i] = new OSMModel.Triangle(apex[i], left[i], right[i]);
                leftAngle[i] = triangle[i].leftAngle();
                rightAngle[i] = triangle[i].rightAngle();
                apexAngle[i] = triangle[i].apexAngle();
                double total = leftAngle[i] + rightAngle[i] + apexAngle[i];
                assertThat("Should sum to 180", total, closeTo(180.0, 0.001));
                //System.out.println("X:" + x + "\tProj:" + triangle[i].project() + "\tleft:" + leftAngle[i] + "\tright:" + rightAngle[i] + "\tapex:" + apexAngle[i]);
                if (i == 0) {
                    double[] projected = triangle[i].project().coordinate();
                    assertThat("Projection should lie on z axis with same value of x as apex", projected[0], equalTo((double) x));
                    assertThat("Projection should lie on z axis with same value of x as apex", projected[1], equalTo(0.0));
                }
                if (i == 1) {
                    double[] projected = triangle[i].project().coordinate();
                    assertThat("Projection should lie on the 1:-1 diagonal with x and y being -10 from apex", projected[0], equalTo(apex[i].coordinate()[0] - 10));
                    assertThat("Projection should lie on the 1:-1 diagonal with x and y being -10 from apex", projected[1], equalTo(apex[i].coordinate()[1] - 10));
                }
                if (i > 0) {
                    assertThat("Should have same left angle", leftAngle[i], closeTo(leftAngle[0], 0.001));
                    assertThat("Should have same right angle", rightAngle[i], closeTo(rightAngle[0], 0.001));
                    assertThat("Should have same apex angle", apexAngle[i], closeTo(apexAngle[0], 0.001));
                }
            }
        }
    }
}
