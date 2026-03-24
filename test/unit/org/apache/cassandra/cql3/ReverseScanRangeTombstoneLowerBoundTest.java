/*
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
package org.apache.cassandra.cql3;

import java.util.Date;
import java.util.UUID;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;

/**
 * Reproduces AssertionError in UnfilteredRowIteratorWithLowerBound.computeNext():
 *   "Lower bound [SSTABLE_UPPER_BOUND(...)] is bigger than first returned value
 *    [Marker INCL_END_BOUND()@643/...]"
 *
 * Seed 598635288979916L. Failing query: reverse scan of partition C.
 *
 * The trigger is ts=523: DELETE ... WHERE ck0=895752309745821669 AND ck1 >= 0.90358263
 * on partition C. This open-ended RT (no upper bound on ck1) causes
 * artificialLowerBound(isReversed=true) to produce SSTABLE_UPPER_BOUND(non-empty),
 * which is greater than the INCL_END_BOUND(TOP) emitted first by the reversed scan.
 */
public class ReverseScanRangeTombstoneLowerBoundTest extends CQLTester
{
    // Partition A
    private static final byte PK0_A = (byte) -103;
    private static final int PK1_A = -643384731;
    private static final UUID PK2_A = UUID.fromString("09fb144f-67cb-4a18-bbbb-e743ee45f9c1");
    private static final int PK3_A = -1155995698;
    private static final String PK4_A = "eulfxcdk";
    private static final long PK5_A = -3444864769623415526L;
    private static final byte PK6_A = (byte) 1;

    // Partition B
    private static final byte PK0_B = (byte) -114;
    private static final int PK1_B = 624010917;
    private static final UUID PK2_B = UUID.fromString("2206f4fe-c335-4a62-b7f6-06c00a3f0155");
    private static final int PK3_B = -35032363;
    private static final String PK4_B = "chwnmsb";
    private static final long PK5_B = -7298089675864906276L;
    private static final byte PK6_B = (byte) 98;

    // Partition C — the failing partition
    private static final byte PK0_C = (byte) -123;
    private static final int PK1_C = -71982420;
    private static final UUID PK2_C = UUID.fromString("c2bc386c-a941-492a-8b1e-371621382081");
    private static final int PK3_C = -1438530849;
    private static final String PK4_C = "rabxbsfc";
    private static final long PK5_C = -6396302275588364261L;
    private static final byte PK6_C = (byte) -46;

    // Partition D
    private static final byte PK0_D = (byte) -123;
    private static final int PK1_D = -426472659;
    private static final UUID PK2_D = UUID.fromString("ef768417-92a7-4029-b59d-ea3be5473397");
    private static final int PK3_D = -2045241439;
    private static final String PK4_D = "ogfbgfwy";
    private static final long PK5_D = -1806653902136603222L;
    private static final byte PK6_D = (byte) 110;

    // Partition E
    private static final byte PK0_E = (byte) -123;
    private static final int PK1_E = 1984024140;
    private static final UUID PK2_E = UUID.fromString("c1b17317-be08-4f7e-9062-955cff8cfe69");
    private static final int PK3_E = -800189878;
    private static final String PK4_E = "mvnhxr";
    private static final long PK5_E = -4366362663150304112L;
    private static final byte PK6_E = (byte) 103;

    @BeforeClass
    public static void setUpClass()
    {
        CassandraRelevantProperties.CURSOR_COMPACTION_ENABLED.setBoolean(false);
        CQLTester.setUpClass();
        DatabaseDescriptor.setColumnIndexSizeInKiB(1);
        DatabaseDescriptor.setColumnIndexCacheSize(1);
    }

    @Before
    public void setUp() throws Throwable
    {
        createTable(
        "CREATE TABLE %s (" +
        "  pk0 tinyint, pk1 int, pk2 uuid, pk3 int, pk4 ascii, pk5 bigint, pk6 tinyint," +
        "  ck0 timestamp, ck1 float, ck2 uuid, ck3 timestamp, ck4 timestamp, ck5 double, ck6 timestamp," +
        "  static0 int static, static1 double static, static2 text static," +
        "  static3 timestamp static, static4 ascii static, static5 timestamp static," +
        "  regular0 bigint, regular1 int, regular2 text, regular3 ascii," +
        "  PRIMARY KEY ((pk0,pk1,pk2,pk3,pk4,pk5,pk6), ck0,ck1,ck2,ck3,ck4,ck5,ck6)" +
        ") WITH CLUSTERING ORDER BY (ck0 DESC,ck1 ASC,ck2 ASC,ck3 ASC,ck4 DESC,ck5 DESC,ck6 DESC)" +
        "   AND gc_grace_seconds = 0" +
        "   AND compression = {'class':'LZ4Compressor','chunk_length_in_kb':'4'}" +
        "   AND compaction = {'class':'SizeTieredCompactionStrategy','enabled':false}");
    }

    /**
     * regular2 and static2 are text; placeholder 'x' is used — content does not affect the bug.
     */
    private void ins(long ts,
                     byte pk0, int pk1, UUID pk2, int pk3, String pk4, long pk5, byte pk6,
                     long ck0, float ck1, UUID ck2, long ck3, long ck4, double ck5, long ck6,
                     long r0, int r1, String r3,
                     int s0, double s1, long s3, String s4, long s5) throws Throwable
    {
        execute(
        "INSERT INTO %s (pk0,pk1,pk2,pk3,pk4,pk5,pk6," +
        "ck0,ck1,ck2,ck3,ck4,ck5,ck6," +
        "regular0,regular1,regular2,regular3," +
        "static0,static1,static2,static3,static4,static5) " +
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'x',?,?,?,'x',?,?,?) USING TIMESTAMP " + ts,
        pk0, pk1, pk2, pk3, pk4, pk5, pk6,
        new Date(ck0), ck1, ck2, new Date(ck3), new Date(ck4), ck5, new Date(ck6),
        r0, r1, r3,
        s0, s1, new Date(s3), s4, new Date(s5));
    }

    @Test
    public void testReverseScanWithOpenEndedRangeTombstone() throws Throwable
    {
        // ===== pa-1 =====
        ins(0, PK0_A, PK1_A, PK2_A, PK3_A, PK4_A, PK5_A, PK6_A,
            7632727128560753908L, 0.86056566f, UUID.fromString("6d811fff-1ae9-4629-9cb3-c073a3899bd7"),
            2552783423828524670L, 540041998414555281L, 0.7816779852562368, 5489130762484314040L,
            538137450356175883L, -167749837, "lidwebtjn",
            -1149603166, 0.1923595199186906, 6343599562721743399L, "havsyybb", 7575898953768982561L);

        execute("DELETE FROM %s USING TIMESTAMP 1 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4=? AND ck5 > ?",
                PK0_B, PK1_B, PK2_B, PK3_B, PK4_B, PK5_B, PK6_B,
                new Date(3530554089686794425L), 0.46891975f,
                UUID.fromString("b651dbbb-03df-4bb5-a863-ffb9a369e7ab"),
                new Date(1287342758679584722L), new Date(6099442158935259219L), 0.612845142205672);

        flush();

        // ===== pa-2 =====
        ins(9, PK0_D, PK1_D, PK2_D, PK3_D, PK4_D, PK5_D, PK6_D,
            5819899907329289192L, 0.5732795f, UUID.fromString("8d7f0bc0-a75f-4b0c-956a-169e3c90badd"),
            2417281081909615725L, 2344584294106171079L, 0.281865523468956, 880245127700417370L,
            -1360510159736478822L, 119534805, "lyukowmq",
            -1764187734, 0.9685315953315764, 2713610434292900120L, "jnuliso", 2410713091547263782L);

        ins(15, PK0_A, PK1_A, PK2_A, PK3_A, PK4_A, PK5_A, PK6_A,
            812178380755637788L, 0.19954473f, UUID.fromString("2d3bcd4c-5591-462c-8530-825d8d24fe32"),
            2817431207435594713L, 5879884394154861614L, 0.9041778226943669, 6962938528450013746L,
            5009396114413956847L, 780223270, "uiavmvy",
            -1754846130, 0.2963675384791413, 1925927083550802744L, "apovg", 6265448369413759012L);

        ins(16, PK0_B, PK1_B, PK2_B, PK3_B, PK4_B, PK5_B, PK6_B,
            3154131362005605271L, 0.50773f, UUID.fromString("e8b162f0-bf49-45f5-a555-229071f971ce"),
            5680709073443119787L, 1209459349905189341L, 0.10511216164678161, 6094843413164142625L,
            -1403470183268682132L, -2002204116, "iwlhwrlyl",
            1388470760, 0.47384308222205607, 4910061654227365998L, "bncqg", 3340887821828193566L);

        execute("DELETE FROM %s USING TIMESTAMP 17 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4 >= ? AND ck4 < ?",
                PK0_C, PK1_C, PK2_C, PK3_C, PK4_C, PK5_C, PK6_C,
                new Date(7983587693192248130L), 0.1913073f,
                UUID.fromString("c794be53-5943-4399-b135-eca7bd40b38e"),
                new Date(7449952831176455238L), new Date(7480853520847543271L), new Date(3524678674385185987L));

        ins(18, PK0_A, PK1_A, PK2_A, PK3_A, PK4_A, PK5_A, PK6_A,
            3277303777923505655L, 0.92245114f, UUID.fromString("5b453822-71fb-4363-ac97-00672bef5bb4"),
            2681483814600787312L, 325598474752323180L, 0.3368445556789259, 1630904462702955624L,
            -4040961557451184053L, -782062293, "amicumr",
            -745395633, 0.9392328662103486, 6343599562721743399L, "umnfwhvta", 5694342579127521537L);

        ins(21, PK0_C, PK1_C, PK2_C, PK3_C, PK4_C, PK5_C, PK6_C,
            1198555696041864807L, 0.05360371f, UUID.fromString("a3194b6c-dcad-45c4-bcfd-ce7b1166f9f9"),
            3567313105247075523L, 9030161708007871151L, 0.24424577436254813, 2103808420568878265L,
            7474661771332548162L, 648761255, "xfthqk",
            -1518748838, 0.45613961510413414, 7153280641827589874L, "gndonjx", 6265448369413759012L);
        
        flush();

        // ===== pa-3 =====
        execute("DELETE FROM %s USING TIMESTAMP 28 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0 >= ?",
                PK0_E,PK1_E,PK2_E,PK3_E,PK4_E,PK5_E,PK6_E, new Date(895752309745821669L));

        ins(31, PK0_A,PK1_A,PK2_A,PK3_A,PK4_A,PK5_A,PK6_A,
            2809388297190438194L, 0.58842385f, UUID.fromString("8995c669-7013-494b-8eaa-71123bc163e3"),
            4505845769425413445L, 2380943868211379170L, 0.3913491188570952, 7158536575603078727L,
            8999686608295900691L, -1556583767, "xpmoj",
            -1764187734, 0.8052847858025086, 4387974667178108118L, "odnuvlehb", 2080391150464250751L);
        
        flush();

        // ===== pa-4 =====
        ins(34, PK0_D,PK1_D,PK2_D,PK3_D,PK4_D,PK5_D,PK6_D,
            31575871650152433L, 0.6400059f, UUID.fromString("59f1295e-9b91-408c-a831-1f4ce979ed47"),
            1575901953351712599L, 8231099604976814110L, 0.6311777133703499, 1495447381079163421L,
            -8423184196074482743L, 1700357258, "oagjywki",
            239963196, 0.15682093518734375, 806659023770351125L, "ovmdtw", 5414576045771357059L);

        ins(37, PK0_C,PK1_C,PK2_C,PK3_C,PK4_C,PK5_C,PK6_C,
            7758678017172239715L, 0.94963646f, UUID.fromString("9b9af73f-184f-4925-812b-799be541c3e8"),
            5267737720135318876L, 5737101732580574276L, 0.713422590290843, 1271608747715662812L,
            -6644988334903443987L, -1177991472, "gajji",
            1641153016, 0.3206979518177019, 2420289100981503227L, "hqpoh", 4463558639681463977L);

        execute("DELETE FROM %s USING TIMESTAMP 42 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4=? AND ck5=? AND ck6=?",
                PK0_B,PK1_B,PK2_B,PK3_B,PK4_B,PK5_B,PK6_B,
                new Date(1255940404591624723L), 0.35399938f,
                UUID.fromString("7bfc40f2-ba61-4d44-a808-67fd474d121e"),
                new Date(3213671399070298611L), new Date(873901181423390753L),
                0.8131888683947316, new Date(6210264356441707874L));

        ins(46, PK0_A,PK1_A,PK2_A,PK3_A,PK4_A,PK5_A,PK6_A,
            3712741308243752409L, 0.1464836f, UUID.fromString("c6e0fbcf-bddd-44b3-958c-1bc4bf243413"),
            6826060682196423849L, 6521186601775352060L, 0.2144181360146188, 7875653929041265449L,
            6013596002619984459L, -363361539, "jsxnptfq",
            -629459200, 0.707941937286291, 6242287956098769446L, "dvila", 6518007602430120295L);

        ins(47, PK0_B,PK1_B,PK2_B,PK3_B,PK4_B,PK5_B,PK6_B,
            4632193635391596954L, 0.31070352f, UUID.fromString("97574f57-399f-4523-ba6a-49d997191327"),
            3947262888313419321L, 4359325211880868133L, 0.8784679441870984, 4876780558082913767L,
            -3542215332208061904L, -278776647, "ydycgl",
            1087335214, 1.6958649554299488E-4, 418623033704222006L, "aodcj", 2603829970740858348L);

        ins(53, PK0_A,PK1_A,PK2_A,PK3_A,PK4_A,PK5_A,PK6_A,
            499626089723261250L, 0.9176857f, UUID.fromString("7410f6a2-9476-41e7-aec1-cf40ff10b39e"),
            9153227693771217707L, 8834216817869058060L, 0.6089453863962033, 990551735786395969L,
            -4346382765124629944L, 601154184, "ixeonesq",
            -735070882, 0.3206979518177019, 8574785080773170455L, "ajaanux", 6598314740372474360L);

        flush();

        // ===== pa-6 =====
        execute("DELETE FROM %s USING TIMESTAMP 60 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0 >= ?",
                PK0_B,PK1_B,PK2_B,PK3_B,PK4_B,PK5_B,PK6_B, new Date(7422066836975833400L));

        execute("DELETE FROM %s USING TIMESTAMP 64 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4=? AND ck5=? AND ck6 > ? AND ck6 < ?",
                PK0_B,PK1_B,PK2_B,PK3_B,PK4_B,PK5_B,PK6_B,
                new Date(8599581541644395765L), 0.08112365f,
                UUID.fromString("e5e90618-ef47-49bf-b3e3-65fb0bf43888"),
                new Date(1747469978537643426L), new Date(2793629306609562600L),
                0.9361004115453935, new Date(214647698174086741L), new Date(7348727968535090653L));

        execute("DELETE FROM %s USING TIMESTAMP 65 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4=? AND ck5=? AND ck6=?",
                PK0_E,PK1_E,PK2_E,PK3_E,PK4_E,PK5_E,PK6_E,
                new Date(4181777546326856279L), 0.68629456f,
                UUID.fromString("9dc378c3-8cfd-4636-b3d3-e3ce296352aa"),
                new Date(5606861737812623902L), new Date(1821948933589284884L),
                0.7190737329990968, new Date(8638229264507788281L));

        ins(68, PK0_C,PK1_C,PK2_C,PK3_C,PK4_C,PK5_C,PK6_C,
            1255940404591624723L, 0.35399938f, UUID.fromString("7bfc40f2-ba61-4d44-a808-67fd474d121e"),
            3213671399070298611L, 873901181423390753L, 0.8131888683947316, 6210264356441707874L,
            -1387940680336308688L, -1556583767, "uiavmvy",
            -701665770, 0.5568927343562914, 2713610434292900120L, "xireftxu", 3428926298664110771L);

        ins(69, PK0_D,PK1_D,PK2_D,PK3_D,PK4_D,PK5_D,PK6_D,
            3663771440737928020L, 0.8861742f, UUID.fromString("8aea71eb-d9b8-40e3-87db-41281b57865e"),
            7559617047240140545L, 9129367344068586710L, 0.586569089666871, 6752871757960909080L,
            6235230387129861794L, -1270126615, "sdouexqn",
            411482786, 0.6554929086211027, 345108373092767828L, "bncqg", 6077144016786437581L);

        ins(73, PK0_B,PK1_B,PK2_B,PK3_B,PK4_B,PK5_B,PK6_B,
            3154131362005605271L, 0.50773f, UUID.fromString("e8b162f0-bf49-45f5-a555-229071f971ce"),
            5680709073443119787L, 1209459349905189341L, 0.10511216164678161, 6094843413164142625L,
            3357339596975630851L, -469958493, "eunjmdaa",
            -1830773478, 0.1368904964631943, 2160488083901027108L, "sjnxx", 2410713091547263782L);

        ins(74, PK0_A,PK1_A,PK2_A,PK3_A,PK4_A,PK5_A,PK6_A,
            6500789125154428590L, 0.24412262f, UUID.fromString("90bdaef3-a9cd-41d2-9534-a86860403996"),
            7177754381874279647L, 1100329779218904869L, 0.7806911815651276, 4058285502384290906L,
            538137450356175883L, -298776597, "didnhgnv",
            808426127, 0.9642100057576731, 2999722620956522547L, "kejdl", 2818387576096704167L);

        ins(76, PK0_C,PK1_C,PK2_C,PK3_C,PK4_C,PK5_C,PK6_C,
            5933238244278998195L, 0.5525186f, UUID.fromString("3f73d34d-f7b0-4297-98c9-36501588c2c8"),
            3265534794813778586L, 6061584621833981114L, 0.36830309326038213, 2927894523922161637L,
            -2162690793396962771L, 2136570315, "ppnuqtkp",
            496306431, 0.04740482332154616, 8221463704771932732L, "kjryof", 7015711960780955191L);

        ins(78, PK0_A,PK1_A,PK2_A,PK3_A,PK4_A,PK5_A,PK6_A,
            7422066836975833400L, 0.89402765f, UUID.fromString("d0a48a4c-a9e6-48b1-a489-906d33b3cc05"),
            1369538066989552306L, 2243569679293359652L, 0.2705296498459, 8887994896905023309L,
            2378875053573902771L, -1932693499, "ekotju",
            -197149523, 0.3014857733966231, 2129397348230857988L, "gtgbr", 9173304307399194982L);

        ins(79, PK0_A,PK1_A,PK2_A,PK3_A,PK4_A,PK5_A,PK6_A,
            5593685444496815230L, 0.569474f, UUID.fromString("30507668-40bb-4236-8f77-7848b96d9a0f"),
            7946049159553714941L, 2507351037773599144L, 0.6343057726047, 44046689115618320L,
            -4040961557451184053L, 1292627168, "iwlhwrlyl",
            1997544066, 0.04740482332154616, 4298635430105934935L, "qhfgkl", 8104235012848134532L);

        execute("DELETE FROM %s USING TIMESTAMP 81 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4=? AND ck5=? AND ck6=?",
                PK0_E,PK1_E,PK2_E,PK3_E,PK4_E,PK5_E,PK6_E,
                new Date(1971468881480320306L), 0.81026834f,
                UUID.fromString("86dc21fe-288a-4cdd-ae98-d010d99ef88b"),
                new Date(8129812276225968468L), new Date(3095561002072516857L),
                0.2405041587925386, new Date(3592289616400232348L));

        ins(86, PK0_E,PK1_E,PK2_E,PK3_E,PK4_E,PK5_E,PK6_E,
            4800135805541885690L, 0.668106f, UUID.fromString("b5269ba9-38ae-42e1-87f7-e3ef3f7aafd1"),
            1043303864092184203L, 3387188141348146810L, 0.8603391792624008, 2478863737136921293L,
            3357339596975630851L, -385976444, "alhvhk",
            -365030483, 0.1777447510363952, 3424612291952549721L, "ljymk", 2080391150464250751L);

        ins(92, PK0_D,PK1_D,PK2_D,PK3_D,PK4_D,PK5_D,PK6_D,
            4800135805541885690L, 0.668106f, UUID.fromString("b5269ba9-38ae-42e1-87f7-e3ef3f7aafd1"),
            1043303864092184203L, 3387188141348146810L, 0.8603391792624008, 2478863737136921293L,
            -9054143081040987668L, -469958493, "kmnuqe",
            -1754846130, 0.7018382233655658, 5187812060189699494L, "qvtxfavr", 9211483162482858808L);

        ins(94, PK0_B,PK1_B,PK2_B,PK3_B,PK4_B,PK5_B,PK6_B,
            1079028068974742532L, 0.5612422f, UUID.fromString("062bf748-76cb-49a5-a683-f309be30e3b1"),
            1860011166045514480L, 1698899552302820345L, 0.7336355420239814, 6604622076713090327L,
            -4179670244210246046L, 795928856, "kcwedwquy",
            -701665770, 0.4123138916168718, 1326968266656024507L, "looiyd", 4945018588128193274L);
        
            // ===== compact pa-1..pa-4 → pa-5 =====
            compact();

            // ===== pa-6 =====
            ins(99, PK0_A,PK1_A,PK2_A,PK3_A,PK4_A,PK5_A,PK6_A,
                1561242477162331711L, 0.13015294f, UUID.fromString("5a19744d-6f30-473e-a8d2-5f83e7739c68"),
                3399477554323743181L, 8168770408242915486L, 0.8719289310524072, 2758198505370642268L,
                -4179670244210246046L, -1556583767, "eunjmdaa",
                195903575, 0.34381164074108905, 1682818105736464041L, "kjryof", 2237846895821823134L);

            ins(105, PK0_B,PK1_B,PK2_B,PK3_B,PK4_B,PK5_B,PK6_B,
                3530554089686794425L, 0.46891975f, UUID.fromString("b651dbbb-03df-4bb5-a863-ffb9a369e7ab"),
                1287342758679584722L, 6099442158935259219L, 0.612845142205672, 3166827539894097890L,
                -4040961557451184053L, -278776647, "huebmq",
                496306431, 0.9685315953315764, 8413680767948360726L, "inwaseqxb", 8596977638084543129L);

            execute("DELETE FROM %s USING TIMESTAMP 107 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                    " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4=? AND ck5=? AND ck6=?",
                    PK0_C,PK1_C,PK2_C,PK3_C,PK4_C,PK5_C,PK6_C,
                    new Date(2025884524768071530L), 0.93594855f,
                    UUID.fromString("6371f229-c3dc-4d22-89aa-0f60d6f0d4fc"),
                    new Date(4541444468319141800L), new Date(6057419655200923174L),
                    0.5871303857607996, new Date(9055645721083334602L));

            ins(109, PK0_D,PK1_D,PK2_D,PK3_D,PK4_D,PK5_D,PK6_D,
                4800135805541885690L, 0.668106f, UUID.fromString("b5269ba9-38ae-42e1-87f7-e3ef3f7aafd1"),
                1043303864092184203L, 3387188141348146810L, 0.8603391792624008, 2478863737136921293L,
                3837039669255549446L, 1700357258, "didnhgnv",
                -44823653, 0.7850442522334002, 3961815471135645233L, "ffayuan", 3889103742075151192L);

            ins(115, PK0_C,PK1_C,PK2_C,PK3_C,PK4_C,PK5_C,PK6_C,
                9125060744247554035L, 0.1791839f, UUID.fromString("61a66418-0720-4a41-8a25-bd9c82fa09d8"),
                6229970420013192421L, 7090408007629952784L, 0.7403983494999508, 5131515571388773512L,
                7431157094306167367L, -1943895741, "qrtpcpb",
                1140804969, 0.6455230265926899, 8762018365937927859L, "bkhaj", 3858616978745197017L);

            ins(116, PK0_A,PK1_A,PK2_A,PK3_A,PK4_A,PK5_A,PK6_A,
                4602305859228747389L, 0.23279327f, UUID.fromString("cf1887ff-12b1-4cf2-8ea6-79c9e782a538"),
                7637179545001983636L, 508818385932100376L, 0.40260742484128265, 87495645225882758L,
                -922860269683984248L, -1711430715, "avdwpwt",
                1553090433, 0.13449966921610157, 1642876432326974601L, "bncqg", 3889103742075151192L);

            execute("DELETE FROM %s USING TIMESTAMP 122 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                    " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4 < ?",
                    PK0_D,PK1_D,PK2_D,PK3_D,PK4_D,PK5_D,PK6_D,
                    new Date(5313340105394188214L), 0.67673683f,
                    UUID.fromString("41142235-7e2a-4cc6-89f1-082b3e4ca9e3"),
                    new Date(2145587868427495618L), new Date(7770461237366671255L));

            ins(129, PK0_C,PK1_C,PK2_C,PK3_C,PK4_C,PK5_C,PK6_C,
                812178380755637788L, 0.19954473f, UUID.fromString("2d3bcd4c-5591-462c-8530-825d8d24fe32"),
                2817431207435594713L, 5879884394154861614L, 0.9041778226943669, 6962938528450013746L,
                5939124363484848629L, 832438403, "iphurudi",
                -1525078101, 0.1368904964631943, 3424612291952549721L, "wdadtw", 5939177749948882276L);

            ins(133, PK0_A,PK1_A,PK2_A,PK3_A,PK4_A,PK5_A,PK6_A,
                6388723533010392664L, 0.37771142f, UUID.fromString("f5506fef-e152-4be7-9984-d72894554cc7"),
                5156203116625725911L, 1583317996109138905L, 0.9835882993578348, 7085699576657174062L,
                -2897961170603275922L, 398262149, "ydycgl",
                -745395633, 0.279047802612038, 1326968266656024507L, "gluyuwte", 4047523402983762181L);

            execute("DELETE FROM %s USING TIMESTAMP 136 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?",
                    PK0_A,PK1_A,PK2_A,PK3_A,PK4_A,PK5_A,PK6_A);

            ins(137, PK0_D,PK1_D,PK2_D,PK3_D,PK4_D,PK5_D,PK6_D,
                7732277659165339601L, 0.07786834f, UUID.fromString("74e464ad-b7fb-45ff-87af-0577a000515c"),
                4051168426074289602L, 8599079740564042389L, 0.18594437975524103, 5015712170813548461L,
                -7490936582026173160L, -185154532, "vijpsfcjf",
                -1986215811, 0.7018382233655658, 1537715395434023062L, "ilhihj", 8104235012848134532L);

            ins(140, PK0_C,PK1_C,PK2_C,PK3_C,PK4_C,PK5_C,PK6_C,
                7413794634167534359L, 0.99083495f, UUID.fromString("d8bb0236-1ec5-4b3c-86a3-161dc998acd0"),
                2406772278510441776L, 9109664813619459892L, 0.3639777902383512, 3667247356397672017L,
                724570375742944143L, -1365578283, "hjjopb",
                -1450450407, 0.2367592909929076, 2176371153412431808L, "wphnqwys", 8596977638084543129L);

            execute("DELETE FROM %s USING TIMESTAMP 143 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                    " AND ck0=? AND ck1 > ? AND ck1 <= ?",
                    PK0_D,PK1_D,PK2_D,PK3_D,PK4_D,PK5_D,PK6_D,
                    new Date(289175317623305456L), 0.011407256f, 0.16604358f);

            ins(144, PK0_B,PK1_B,PK2_B,PK3_B,PK4_B,PK5_B,PK6_B,
                8068888305962221487L, 0.71878743f, UUID.fromString("b6aa919f-3b4a-4718-8559-0bc231f6a1f5"),
                4175554469274387962L, 3325823121555814239L, 0.30827012484740146, 1708633450061016010L,
                5939124363484848629L, 1128390818, "csfjrajsj",
                -2048007240, 0.45613961510413414, 7350807410879730860L, "gndonjx", 4945018588128193274L);

            execute("DELETE FROM %s USING TIMESTAMP 148 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                    " AND ck0=? AND ck1 > ? AND ck1 <= ?",
                    PK0_D,PK1_D,PK2_D,PK3_D,PK4_D,PK5_D,PK6_D,
                    new Date(6580077920587976138L), 0.6841685f, 0.83093476f);

            ins(154, PK0_C,PK1_C,PK2_C,PK3_C,PK4_C,PK5_C,PK6_C,
                9069551782509199883L, 0.108139575f, UUID.fromString("a8a7e3ed-2b4d-479e-920a-5bbc46d553e2"),
                4302223178633353233L, 7823483121157046262L, 0.16550559082599003, 2610708278172503123L,
                -5885890536825904287L, 343457182, "mjlcko",
                -701665770, 0.2367592909929076, 6571260224488595740L, "qvtxfavr", 1014979746352894530L);

            ins(157, PK0_A,PK1_A,PK2_A,PK3_A,PK4_A,PK5_A,PK6_A,
                7951563981932272765L, 0.33266354f, UUID.fromString("6b33b392-e435-4193-aab9-aa045332312c"),
                7132496354360776139L, 3027207019475536322L, 0.5394878696699469, 1712126919758621283L,
                8912238212434690976L, 96597707, "fuopbjwug",
                -1830773478, 0.5642163691920405, 806659023770351125L, "dvila", 5682306938430493007L);

            ins(158, PK0_C,PK1_C,PK2_C,PK3_C,PK4_C,PK5_C,PK6_C,
                2482567508472125935L, 0.5997127f, UUID.fromString("7ed31494-4243-4544-96e0-97f76b543228"),
                8557611565075851071L, 2318261385773055094L, 0.8467894324650492, 4127637482729249104L,
                -1360510159736478822L, 610190418, "utwpcbt",
                -2056291664, 0.5826582783688924, 26169827018591802L, "wwtctnpmw", 6304567801367375323L);

            flush(); // → pa-6

            // ===== pa-7 =====
            ins(165, PK0_A,PK1_A,PK2_A,PK3_A,PK4_A,PK5_A,PK6_A,
                5593685444496815230L, 0.569474f, UUID.fromString("30507668-40bb-4236-8f77-7848b96d9a0f"),
                7946049159553714941L, 2507351037773599144L, 0.6343057726047, 44046689115618320L,
                -2162690793396962771L, 1550087620, "enmqootg",
                -2055117706, 0.6455230265926899, 7350807410879730860L, "gyffu", 7732337830741508764L);

            ins(166, PK0_D,PK1_D,PK2_D,PK3_D,PK4_D,PK5_D,PK6_D,
                4057746519920827335L, 0.5699378f, UUID.fromString("da85dd5f-b8d1-4919-a220-a24f5f4da476"),
                1556922617641054762L, 8985251458796196883L, 0.6703906842060539, 936047755194417775L,
                3291187951139489121L, -2027921997, "nishlidjy",
                2017663808, 0.5568927343562914, 7746022797742066891L, "apovg", 6294895644797398413L);

            ins(168, PK0_D,PK1_D,PK2_D,PK3_D,PK4_D,PK5_D,PK6_D,
                9176544342293714393L, 0.28937167f, UUID.fromString("96edd406-a2a2-4118-b625-a6fe026bfda2"),
                4686973822019620312L, 1364819553908822931L, 0.4163774190855736, 8204643129472492639L,
                -5541624110829642265L, 246260868, "eunjmdaa",
                -1525078101, 0.9642100057576731, 7200766659155901218L, "fkhiisyw", 3007810890379602014L);

            ins(170, PK0_A,PK1_A,PK2_A,PK3_A,PK4_A,PK5_A,PK6_A,
                2874785372503560757L, 0.6742679f, UUID.fromString("b001d726-d204-4505-bb60-7a5b0bc1cee1"),
                4025585663335504700L, 6246805665195412521L, 0.5900811957624077, 2669245690139052997L,
                -378977582890258498L, 1700357258, "ajjdj",
                276597449, 0.2367592909929076, 1884847131416413805L, "yyhjbva", 5735676138523971223L);

            ins(179, PK0_A,PK1_A,PK2_A,PK3_A,PK4_A,PK5_A,PK6_A,
                4000614813894526888L, 0.1041373f, UUID.fromString("d5b0ce07-75e1-4bfb-a085-68c656bf4e9f"),
                6547761639244433345L, 6336406489859958779L, 0.37419406074676986, 6978774282727005246L,
                1832518556048751082L, -167749837, "dxkxwm",
                -712366567, 0.7654317916033275, 5415349969153321942L, "erxojysku", 4945018588128193274L);

            execute("DELETE FROM %s USING TIMESTAMP 180 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                    " AND ck0=? AND ck1=? AND ck2=? AND ck3 >= ?",
                    PK0_B,PK1_B,PK2_B,PK3_B,PK4_B,PK5_B,PK6_B,
                    new Date(2809388297190438194L), 0.58842385f,
                    UUID.fromString("8995c669-7013-494b-8eaa-71123bc163e3"),
                    new Date(4505845769425413445L));

            ins(181, PK0_B,PK1_B,PK2_B,PK3_B,PK4_B,PK5_B,PK6_B,
                7951717091631179666L, 0.4980042f, UUID.fromString("6f920f1d-c64c-493e-b616-c5ec5efff9dd"),
                7125370822648282997L, 778614162346502814L, 0.5362958207622082, 825473563299689787L,
                -8211060219653533401L, -1831317936, "huebmq",
                620744179, 0.9169147249197589, 26169827018591802L, "veemuwc", 8596977638084543129L);

            ins(185, PK0_A,PK1_A,PK2_A,PK3_A,PK4_A,PK5_A,PK6_A,
                8229295995722248342L, 0.5424265f, UUID.fromString("d5de434b-dd4a-4a94-b33d-db9dc07e2f67"),
                3872139198142639730L, 4894301062569488584L, 0.6311220473311565, 139079661256660300L,
                958757422847071162L, 810773963, "aqhaay",
                654190597, 0.19704888007978127, 6176885477697750333L, "kalkbma", 910870060698347176L);

            ins(188, PK0_B,PK1_B,PK2_B,PK3_B,PK4_B,PK5_B,PK6_B,
                2475708391698712270L, 0.88850486f, UUID.fromString("9b891a5b-f1c4-44e3-b6fc-f8660e2149b9"),
                133813649090588046L, 4695436190085171152L, 0.3549370649586403, 8497560302348815810L,
                -8555707246649183016L, -1997667016, "plgiegv",
                1843794304, 0.8779346958528811, 2966349122800837073L, "koaeejbyc", 8574350637684827920L);

            ins(190, PK0_B,PK1_B,PK2_B,PK3_B,PK4_B,PK5_B,PK6_B,
                4632193635391596954L, 0.31070352f, UUID.fromString("97574f57-399f-4523-ba6a-49d997191327"),
                3947262888313419321L, 4359325211880868133L, 0.8784679441870984, 4876780558082913767L,
                -8423184196074482743L, 1306491370, "didnhgnv",
                1675762480, 0.6554929086211027, 6343199139495133846L, "roglltv", 4047523402983762181L);

            ins(196, PK0_D,PK1_D,PK2_D,PK3_D,PK4_D,PK5_D,PK6_D,
                7983587693192248130L, 0.1913073f, UUID.fromString("c794be53-5943-4399-b135-eca7bd40b38e"),
                7449952831176455238L, 7480853520847543271L, 0.5449490541172854, 2989961733792502196L,
                -3542215332208061904L, -1166959004, "oqmdmfs",
                -2086869212, 0.49770734688869733, 3424612291952549721L, "aelffi", 4167675248006084881L);

            execute("DELETE FROM %s USING TIMESTAMP 204 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                    " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4=? AND ck5=? AND ck6=?",
                    PK0_D,PK1_D,PK2_D,PK3_D,PK4_D,PK5_D,PK6_D,
                    new Date(2283876992331598438L), 0.827251f,
                    UUID.fromString("daa5ac41-bca8-4df7-b0c5-dcd5bc117c8f"),
                    new Date(4469256087089103089L), new Date(3076538037675387442L),
                    0.44640869773427583, new Date(5757916812157717748L));

            execute("DELETE FROM %s USING TIMESTAMP 205 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                    " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4=? AND ck5=? AND ck6=?",
                    PK0_D,PK1_D,PK2_D,PK3_D,PK4_D,PK5_D,PK6_D,
                    new Date(7720752672133646765L), 0.98094994f,
                    UUID.fromString("d82b2a80-dba2-4bbd-86ba-5ddb0ffc9f91"),
                    new Date(1537578985926481475L), new Date(3524678674385185987L),
                    0.8792233897278756, new Date(6538962615637346674L));

            ins(206, PK0_D,PK1_D,PK2_D,PK3_D,PK4_D,PK5_D,PK6_D,
                6388723533010392664L, 0.37771142f, UUID.fromString("f5506fef-e152-4be7-9984-d72894554cc7"),
                5156203116625725911L, 1583317996109138905L, 0.9835882993578348, 7085699576657174062L,
                1866143645947822265L, -298776597, "mahsetv",
                -1584427952, 0.45613961510413414, 2966349122800837073L, "qnvyv", 9173304307399194982L);

            ins(211, PK0_C,PK1_C,PK2_C,PK3_C,PK4_C,PK5_C,PK6_C,
                7632727128560753908L, 0.86056566f, UUID.fromString("6d811fff-1ae9-4629-9cb3-c073a3899bd7"),
                2552783423828524670L, 540041998414555281L, 0.7816779852562368, 5489130762484314040L,
                -2162690793396962771L, -506938268, "reframawc",
                173895394, 0.04740482332154616, 6147310493264803437L, "xgrenecs", 1325354184624860540L);

            execute("DELETE FROM %s USING TIMESTAMP 215 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                    " AND ck0=? AND ck1=? AND ck2=? AND ck3 <= ?",
                    PK0_D,PK1_D,PK2_D,PK3_D,PK4_D,PK5_D,PK6_D,
                    new Date(8599581541644395765L), 0.08112365f,
                    UUID.fromString("e5e90618-ef47-49bf-b3e3-65fb0bf43888"),
                    new Date(1747469978537643426L));

            execute("DELETE FROM %s USING TIMESTAMP 218 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                    " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4=? AND ck5=? AND ck6=?",
                    PK0_C,PK1_C,PK2_C,PK3_C,PK4_C,PK5_C,PK6_C,
                    new Date(2025884524768071530L), 0.93594855f,
                    UUID.fromString("6371f229-c3dc-4d22-89aa-0f60d6f0d4fc"),
                    new Date(4541444468319141800L), new Date(6057419655200923174L),
                    0.5871303857607996, new Date(9055645721083334602L));

            flush(); // → pa-7

            // ===== pa-8 =====
            ins(223, PK0_A,PK1_A,PK2_A,PK3_A,PK4_A,PK5_A,PK6_A,
                8089855191693757346L, 0.204027f, UUID.fromString("fc207c79-0185-4175-93f1-995b0664cf81"),
                6820709174589157461L, 2011076679173066352L, 0.22275537525725275, 7590497905193981801L,
                8793748800590335815L, -363361539, "ewfiurubo",
                2049236113, 0.910269862523838, 4670305242640454416L, "mckuesj", 8367469627117874554L);

            ins(224, PK0_C,PK1_C,PK2_C,PK3_C,PK4_C,PK5_C,PK6_C,
                2657673935921402657L, 0.83093476f, UUID.fromString("ca8db170-c26b-4b15-a224-247053ff707c"),
                3354894401425866113L, 6954223544573883077L, 0.8752323329537812, 1789240321272708090L,
                -2836522007065967053L, 1446601079, "ppnuqtkp",
                -869363897, 0.7654317916033275, 2176371153412431808L, "sjnxx", 8104235012848134532L);

            execute("DELETE FROM %s USING TIMESTAMP 226 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                    " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4=? AND ck5=? AND ck6=?",
                    PK0_B,PK1_B,PK2_B,PK3_B,PK4_B,PK5_B,PK6_B,
                    new Date(1026122448593234227L), 0.16962183f,
                    UUID.fromString("c697db11-b843-49f1-994d-f4721b2fcfac"),
                    new Date(2285671487235293577L), new Date(3471857804882515297L),
                    0.06149699724893187, new Date(7608240857631165279L));

            ins(230, PK0_E,PK1_E,PK2_E,PK3_E,PK4_E,PK5_E,PK6_E,
                2683398506297989715L, 0.11806649f, UUID.fromString("3083b908-4d6a-4c25-a637-5c5b03c3a4f5"),
                3439121668211859311L, 3428968279530464015L, 0.058674302466495964, 7173402927535096492L,
                -7234497152975857024L, 993970894, "egdnquj",
                -1802108096, 0.060410806018144636, 8302072647452597106L, "gluyuwte", 6351552073225373778L);

        ins(234, PK0_E,PK1_E,PK2_E,PK3_E,PK4_E,PK5_E,PK6_E,
            9022428633928934563L, 0.56795704f, UUID.fromString("39e186c2-c8f7-487a-920a-6a3a7f8d0361"),
            9003116159877849540L, 3458561709672412820L, 0.6791694292210128, 993608413089858709L,
            3357339596975630851L, -1341206773, "npwbd",
            -1589414833, 0.1404304302470194, 3567305114876562776L, "addcjn", 290180390720296475L);

        ins(236, PK0_B,PK1_B,PK2_B,PK3_B,PK4_B,PK5_B,PK6_B,
            6111890564137593170L, 0.5738175f, UUID.fromString("7ecbd5a0-d306-48b7-b8eb-80aafff55a4f"),
            4897981414968920221L, 8268830877460161659L, 0.780072481444242, 3231222035168309292L,
            5213448789840841987L, 1755569234, "qtnit",
            1997544066, 0.3037955602456479, 5578786084678969460L, "ajaanux", 8029524648915357594L);

        execute("DELETE FROM %s USING TIMESTAMP 240 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0 <= ?",
                PK0_C,PK1_C,PK2_C,PK3_C,PK4_C,PK5_C,PK6_C,
                new Date(3808447273678170459L));

        ins(247, PK0_C,PK1_C,PK2_C,PK3_C,PK4_C,PK5_C,PK6_C,
            4480740696640730387L, 0.99179953f, UUID.fromString("5fc8180d-4b94-4bf3-9c5b-9f6926edd984"),
            3226888215901929740L, 824030616736483447L, 0.8120888396610321, 2062483373773789149L,
            -4040961557451184053L, 398262149, "ewfiurubo",
            -1830773478, 0.3492331736875619, 3870843709397981037L, "roglltv", 3793767186743223272L);

        ins(250, PK0_D,PK1_D,PK2_D,PK3_D,PK4_D,PK5_D,PK6_D,
            4480740696640730387L, 0.99179953f, UUID.fromString("5fc8180d-4b94-4bf3-9c5b-9f6926edd984"),
            3226888215901929740L, 824030616736483447L, 0.8120888396610321, 2062483373773789149L,
            -1387940680336308688L, -1831317936, "uiavmvy",
            -791270996, 0.005054617007972939, 8574785080773170455L, "lkerlhdr", 2818387576096704167L);

        ins(253, PK0_C,PK1_C,PK2_C,PK3_C,PK4_C,PK5_C,PK6_C,
            4537956220047417742L, 0.32449347f, UUID.fromString("1d88fc60-4f10-4d4e-9124-e42042cb1e11"),
            4542789633896780183L, 8874851012397652827L, 0.14732505106046812, 4381764839520968749L,
            -8211060219653533401L, 385125906, "reframawc",
            148954010, 0.6070449986280896, 6176885477697750333L, "bncqg", 7034264759872538228L);

        ins(255, PK0_D,PK1_D,PK2_D,PK3_D,PK4_D,PK5_D,PK6_D,
            2809388297190438194L, 0.58842385f, UUID.fromString("8995c669-7013-494b-8eaa-71123bc163e3"),
            4505845769425413445L, 2380943868211379170L, 0.3913491188570952, 7158536575603078727L,
            -162150703180466019L, -1227570460, "bqwdmtx",
            1087335214, 0.47264369137067774, 3151975458783324981L, "sjnxx", 8104235012848134532L);

        execute("DELETE FROM %s USING TIMESTAMP 256 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2 >= ?",
                PK0_A,PK1_A,PK2_A,PK3_A,PK4_A,PK5_A,PK6_A,
                new Date(3663771440737928020L), 0.8861742f,
                UUID.fromString("8aea71eb-d9b8-40e3-87db-41281b57865e"));

        ins(259, PK0_B,PK1_B,PK2_B,PK3_B,PK4_B,PK5_B,PK6_B,
            2244580056279493209L, 0.49552703f, UUID.fromString("a53e305c-1f57-4126-88a5-98202860a7a3"),
            7508209535294221793L, 4908129738684902195L, 0.7311285640105962, 3331524405908450885L,
            8912238212434690976L, -1831317936, "npwbd",
            1885880434, 0.060410806018144636, 1326968266656024507L, "fkhiisyw", 8893532095607938940L);

        ins(260, PK0_C,PK1_C,PK2_C,PK3_C,PK4_C,PK5_C,PK6_C,
            9176544342293714393L, 0.28937167f, UUID.fromString("96edd406-a2a2-4118-b625-a6fe026bfda2"),
            4686973822019620312L, 1364819553908822931L, 0.4163774190855736, 8204643129472492639L,
            -8556412068641359634L, 832438403, "hckhdu",
            276597449, 0.8189672301048969, 6607201399522087313L, "inwaseqxb", 290180390720296475L);

        ins(262, PK0_A,PK1_A,PK2_A,PK3_A,PK4_A,PK5_A,PK6_A,
            3154131362005605271L, 0.50773f, UUID.fromString("e8b162f0-bf49-45f5-a555-229071f971ce"),
            5680709073443119787L, 1209459349905189341L, 0.10511216164678161, 6094843413164142625L,
            3829496367547203737L, 385125906, "ekotju",
            329664441, 0.21013736125081195, 5838383601286492306L, "hqpoh", 7002741525609526597L);

        execute("DELETE FROM %s USING TIMESTAMP 263 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0 >= ? AND ck0 < ?",
                PK0_D,PK1_D,PK2_D,PK3_D,PK4_D,PK5_D,PK6_D,
                new Date(499626089723261250L), new Date(556007714326422029L));

        ins(265, PK0_E,PK1_E,PK2_E,PK3_E,PK4_E,PK5_E,PK6_E,
            2874785372503560757L, 0.6742679f, UUID.fromString("b001d726-d204-4505-bb60-7a5b0bc1cee1"),
            4025585663335504700L, 6246805665195412521L, 0.5900811957624077, 2669245690139052997L,
            538137450356175883L, 1023960532, "xdwdxdygl",
            -1830773478, 0.8829902715954624, 1682818105736464041L, "nrgtm", 2316584749587265340L);

        ins(266, PK0_D,PK1_D,PK2_D,PK3_D,PK4_D,PK5_D,PK6_D,
            7632727128560753908L, 0.86056566f, UUID.fromString("6d811fff-1ae9-4629-9cb3-c073a3899bd7"),
            2552783423828524670L, 540041998414555281L, 0.7816779852562368, 5489130762484314040L,
            6013596002619984459L, 2060848501, "ewfiurubo",
            411482786, 0.5085083634146282, 856965029349641147L, "kejdl", 5694342579127521537L);

        ins(267, PK0_E,PK1_E,PK2_E,PK3_E,PK4_E,PK5_E,PK6_E,
            4057746519920827335L, 0.5699378f, UUID.fromString("da85dd5f-b8d1-4919-a220-a24f5f4da476"),
            1556922617641054762L, 8985251458796196883L, 0.6703906842060539, 936047755194417775L,
            -1360510159736478822L, -1184093305, "mxsnoknpb",
            411482786, 0.7056614349405654, 6571260224488595740L, "ilhihj", 2479188753372019202L);

        execute("DELETE FROM %s USING TIMESTAMP 277 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4=? AND ck5=? AND ck6=?",
                PK0_C,PK1_C,PK2_C,PK3_C,PK4_C,PK5_C,PK6_C,
                new Date(4000614813894526888L), 0.1041373f,
                UUID.fromString("d5b0ce07-75e1-4bfb-a085-68c656bf4e9f"),
                new Date(6547761639244433345L), new Date(6336406489859958779L),
                0.37419406074676986, new Date(6978774282727005246L));

        execute("DELETE FROM %s USING TIMESTAMP 280 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0 >= ? AND ck0 < ?",
                PK0_E,PK1_E,PK2_E,PK3_E,PK4_E,PK5_E,PK6_E,
                new Date(1971468881480320306L), new Date(8229295995722248342L));

        execute("DELETE FROM %s USING TIMESTAMP 282 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4 > ? AND ck4 <= ?",
                PK0_E,PK1_E,PK2_E,PK3_E,PK4_E,PK5_E,PK6_E,
                new Date(7758678017172239715L), 0.94963646f,
                UUID.fromString("9b9af73f-184f-4925-812b-799be541c3e8"),
                new Date(5267737720135318876L), new Date(5737101732580574276L),
                new Date(203308212341500775L));

        ins(285, PK0_D,PK1_D,PK2_D,PK3_D,PK4_D,PK5_D,PK6_D,
            1296080472623078272L, 0.547035f, UUID.fromString("efa0e319-9735-4c80-b295-42a067f5eabe"),
            6596501982232982068L, 7110419925013858804L, 0.5459652331193541, 1641133290687885470L,
            4823968733380065425L, -2002204116, "qrnnbel",
            2113211474, 0.6037366419515826, 8302072647452597106L, "lrpsxaujd", 290180390720296475L);

        ins(292, PK0_B,PK1_B,PK2_B,PK3_B,PK4_B,PK5_B,PK6_B,
            8154640875635237823L, 0.048169494f, UUID.fromString("bab79e81-e824-4c37-a5a7-f796e6890fd6"),
            6293564583563398520L, 1156441304664637843L, 0.7574712860602678, 8402780122121008721L,
            6704897206942157594L, 1128390818, "ydycgl",
            -17587628, 0.45613961510413414, 359877360643733809L, "rwmkdamyp", 2085124337011873187L);

        ins(296, PK0_E,PK1_E,PK2_E,PK3_E,PK4_E,PK5_E,PK6_E,
            2566152003571977594L, 0.5282622f, UUID.fromString("5247fabe-c3a6-4a28-b779-c1925aa94fc7"),
            4180268629224916553L, 4264960205105382903L, 0.6679116538607843, 7363251670154659579L,
            -4179670244210246046L, 832438403, "eunjmdaa",
            620744179, 0.5515682331226082, 5973263739458729239L, "anphtrnoq", 3858616978745197017L);

        ins(297, PK0_A,PK1_A,PK2_A,PK3_A,PK4_A,PK5_A,PK6_A,
            3097542906811718431L, 0.34555322f, UUID.fromString("ce206765-ebcb-4644-a704-9295c4b4cc5c"),
            6485571002832473405L, 5366803924074360686L, 0.23481937599315805, 4449206729258975303L,
            3837039669255549446L, -768535095, "nishlidjy",
            148954010, 0.1368904964631943, 2420289100981503227L, "looiyd", 4684801409670968841L);

        ins(301, PK0_D,PK1_D,PK2_D,PK3_D,PK4_D,PK5_D,PK6_D,
            2244580056279493209L, 0.49552703f, UUID.fromString("a53e305c-1f57-4126-88a5-98202860a7a3"),
            7508209535294221793L, 4908129738684902195L, 0.7311285640105962, 3331524405908450885L,
            -9054143081040987668L, -1932693499, "reiximiur",
            173470026, 0.9246804071557982, 4408177692550082292L, "ajaanux", 5498506100415819998L);

        ins(302, PK0_C,PK1_C,PK2_C,PK3_C,PK4_C,PK5_C,PK6_C,
            2874785372503560757L, 0.6742679f, UUID.fromString("b001d726-d204-4505-bb60-7a5b0bc1cee1"),
            4025585663335504700L, 6246805665195412521L, 0.5900811957624077, 2669245690139052997L,
            -2230461932206953374L, -185154532, "jmuph",
            -791270996, 0.8779346958528811, 6176885477697750333L, "tvooqggr", 845772459484244853L);

        execute("DELETE FROM %s USING TIMESTAMP 303 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4=? AND ck5 > ? AND ck5 <= ?",
                PK0_A,PK1_A,PK2_A,PK3_A,PK4_A,PK5_A,PK6_A,
                new Date(1255940404591624723L), 0.35399938f,
                UUID.fromString("7bfc40f2-ba61-4d44-a808-67fd474d121e"),
                new Date(3213671399070298611L), new Date(873901181423390753L),
                0.8131888683947316, 0.8362507904816725);

        execute("DELETE FROM %s USING TIMESTAMP 310 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4 > ?",
                PK0_A,PK1_A,PK2_A,PK3_A,PK4_A,PK5_A,PK6_A,
                new Date(9047298101924429479L), 0.5118743f,
                UUID.fromString("65acdb22-d59c-4621-9f59-696d1549d6ec"),
                new Date(2281895331714799583L), new Date(6651124730216992223L));

        ins(311, PK0_D,PK1_D,PK2_D,PK3_D,PK4_D,PK5_D,PK6_D,
            7732277659165339601L, 0.07786834f, UUID.fromString("74e464ad-b7fb-45ff-87af-0577a000515c"),
            4051168426074289602L, 8599079740564042389L, 0.18594437975524103, 5015712170813548461L,
            -8423184196074482743L, -2027921997, "jsxnptfq",
            -712366567, 0.1923595199186906, 5309443160979456609L, "sfqps", 8104235012848134532L);

        ins(314, PK0_D,PK1_D,PK2_D,PK3_D,PK4_D,PK5_D,PK6_D,
            7422066836975833400L, 0.89402765f, UUID.fromString("d0a48a4c-a9e6-48b1-a489-906d33b3cc05"),
            1369538066989552306L, 2243569679293359652L, 0.2705296498459, 8887994896905023309L,
            1866143645947822265L, -1812963328, "gajji",
            -1970035686, 0.15392949489298968, 5737826017827183249L, "ljymk", 6294895644797398413L);

        ins(315, PK0_A,PK1_A,PK2_A,PK3_A,PK4_A,PK5_A,PK6_A,
            4057746519920827335L, 0.5699378f, UUID.fromString("da85dd5f-b8d1-4919-a220-a24f5f4da476"),
            1556922617641054762L, 8985251458796196883L, 0.6703906842060539, 936047755194417775L,
            7030263460795444731L, 810773963, "xeeor",
            -370113554, 0.15392949489298968, 5497038450726882035L, "uwmiwm", 4996864502935100081L);

        ins(316, PK0_E,PK1_E,PK2_E,PK3_E,PK4_E,PK5_E,PK6_E,
            7030544561465643383L, 0.910579f, UUID.fromString("41843ccb-3c51-4026-a191-6e799a78800c"),
            152217791299011442L, 3487853184709005818L, 0.5904247670813827, 3454543472145524600L,
            -2223299001641782934L, -851040014, "npwbd",
            1465837439, 0.15392949489298968, 4670305242640454416L, "berbs", 7034264759872538228L);

        ins(317, PK0_E,PK1_E,PK2_E,PK3_E,PK4_E,PK5_E,PK6_E,
            2475708391698712270L, 0.88850486f, UUID.fromString("9b891a5b-f1c4-44e3-b6fc-f8660e2149b9"),
            133813649090588046L, 4695436190085171152L, 0.3549370649586403, 8497560302348815810L,
            5580848209367617853L, -506938268, "nishlidjy",
            148954010, 0.1923595199186906, 6147310493264803437L, "knono", 7732337830741508764L);

        ins(318, PK0_C,PK1_C,PK2_C,PK3_C,PK4_C,PK5_C,PK6_C,
            7732277659165339601L, 0.07786834f, UUID.fromString("74e464ad-b7fb-45ff-87af-0577a000515c"),
            4051168426074289602L, 8599079740564042389L, 0.18594437975524103, 5015712170813548461L,
            -195839596581881022L, 1755569234, "kcwedwquy",
            -1830773478, 0.9246804071557982, 1884847131416413805L, "looiyd", 6213191237299301044L);

        ins(320, PK0_C,PK1_C,PK2_C,PK3_C,PK4_C,PK5_C,PK6_C,
            5504845159324637795L, 0.713583f, UUID.fromString("2542aa19-7746-49a7-97e9-52be150a4add"),
            8620805869025402241L, 515169708684029143L, 0.46098040746372493, 6857098616492110070L,
            958757422847071162L, 384983922, "ppnuqtkp",
            1379547678, 0.3561561191913202, 5737826017827183249L, "holur", 4155390881488293197L);

        ins(321, PK0_B,PK1_B,PK2_B,PK3_B,PK4_B,PK5_B,PK6_B,
            6500789125154428590L, 0.24412262f, UUID.fromString("90bdaef3-a9cd-41d2-9534-a86860403996"),
            7177754381874279647L, 1100329779218904869L, 0.7806911815651276, 4058285502384290906L,
            -6644988334903443987L, -1132297796, "ydycgl",
            -17587628, 0.6037366419515826, 418623033704222006L, "lkerlhdr", 2603829970740858348L);

        execute("DELETE FROM %s USING TIMESTAMP 323 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1 >= ? AND ck1 < ?",
                PK0_D,PK1_D,PK2_D,PK3_D,PK4_D,PK5_D,PK6_D,
                new Date(2657673935921402657L), 0.83093476f, 0.16604358f);

        execute("DELETE FROM %s USING TIMESTAMP 325 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4=? AND ck5=? AND ck6 > ? AND ck6 <= ?",
                PK0_A,PK1_A,PK2_A,PK3_A,PK4_A,PK5_A,PK6_A,
                new Date(7983587693192248130L), 0.1913073f,
                UUID.fromString("c794be53-5943-4399-b135-eca7bd40b38e"),
                new Date(7449952831176455238L), new Date(7480853520847543271L),
                0.5449490541172854, new Date(2989961733792502196L),
                new Date(7086297271577062651L));

        ins(328, PK0_D,PK1_D,PK2_D,PK3_D,PK4_D,PK5_D,PK6_D,
            6668709495197969234L, 0.7572929f, UUID.fromString("ff5695f4-c409-4d3d-87a3-2b7d56ce2a79"),
            4533561036270685103L, 4001433606903240362L, 0.41279074997146703, 4167647728039974949L,
            -8211060219653533401L, -1812963328, "xeeor",
            173470026, 0.7767799134818635, 636121876247900764L, "umnfwhvta", 910870060698347176L);

        ins(330, PK0_B,PK1_B,PK2_B,PK3_B,PK4_B,PK5_B,PK6_B,
            3487058866579138849L, 0.17002022f, UUID.fromString("b2982a41-6756-40a3-9ba5-d39d9f9a1d3e"),
            4459817351160008903L, 5731511300370199982L, 0.5758867868501081, 7929214166679707890L,
            5283040871071127324L, 1187403325, "kmnuqe",
            2016705295, 0.20963357936071647, 2935710107150620926L, "njysbm", 4684801409670968841L);

        ins(333, PK0_D,PK1_D,PK2_D,PK3_D,PK4_D,PK5_D,PK6_D,
            3235151754271335402L, 0.13368613f, UUID.fromString("b1b4c1ae-0a04-49d7-9d5f-23d70077b50a"),
            5364825071757583699L, 5956849314583722713L, 0.31607461811038184, 6654222012218232137L,
            6447154842953104728L, -1751078269, "eyvrcfwkl",
            -1569777543, 0.7767799134818635, 5497038450726882035L, "inwaseqxb", 5694342579127521537L);

        ins(334, PK0_B,PK1_B,PK2_B,PK3_B,PK4_B,PK5_B,PK6_B,
            1079028068974742532L, 0.5612422f, UUID.fromString("062bf748-76cb-49a5-a683-f309be30e3b1"),
            1860011166045514480L, 1698899552302820345L, 0.7336355420239814, 6604622076713090327L,
            -4346382765124629944L, -1227570460, "xeeor",
            1620873685, 0.7654317916033275, 7571347112083109987L, "lkjghx", 3428926298664110771L);

        ins(338, PK0_D,PK1_D,PK2_D,PK3_D,PK4_D,PK5_D,PK6_D,
            2874785372503560757L, 0.6742679f, UUID.fromString("b001d726-d204-4505-bb60-7a5b0bc1cee1"),
            4025585663335504700L, 6246805665195412521L, 0.5900811957624077, 2669245690139052997L,
            8912238212434690976L, -1264051482, "gajji",
            1885880434, 0.5568927343562914, 7720781628190623601L, "iatfadj", 5840883269182147789L);

        flush(); // → pa-8

        // ===== pa-9 =====
        execute("DELETE FROM %s USING TIMESTAMP 341 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2 > ? AND ck2 <= ?",
                PK0_C,PK1_C,PK2_C,PK3_C,PK4_C,PK5_C,PK6_C,
                new Date(3749303178438710034L), 0.956427f,
                UUID.fromString("cc95649c-4d51-49b6-bc50-faf5b2907894"),
                UUID.fromString("26802f7c-7b83-4e2b-8956-cdecbc91c9d6"));

        ins(346, PK0_C,PK1_C,PK2_C,PK3_C,PK4_C,PK5_C,PK6_C,
            7030544561465643383L, 0.910579f, UUID.fromString("41843ccb-3c51-4026-a191-6e799a78800c"),
            152217791299011442L, 3487853184709005818L, 0.5904247670813827, 3454543472145524600L,
            8999686608295900691L, 385125906, "rbmwq",
            1641153016, 0.7436132271555058, 2176371153412431808L, "wmdrrpck", 2237846895821823134L);

        ins(350, PK0_D,PK1_D,PK2_D,PK3_D,PK4_D,PK5_D,PK6_D,
            8154640875635237823L, 0.048169494f, UUID.fromString("bab79e81-e824-4c37-a5a7-f796e6890fd6"),
            6293564583563398520L, 1156441304664637843L, 0.7574712860602678, 8402780122121008721L,
            -195839596581881022L, 1401208238, "yohvkyyfb",
            -365030483, 0.060410806018144636, 7156904707870505366L, "looiyd", 3007810890379602014L);

        ins(355, PK0_A,PK1_A,PK2_A,PK3_A,PK4_A,PK5_A,PK6_A,
            5981158943882949611L, 0.468988f, UUID.fromString("036ad65f-43db-4249-87b9-85094f3a88dd"),
            5665422587824094996L, 5323609800137447569L, 0.04763477823183204, 3516214253880021644L,
            -162150703180466019L, 1128390818, "hckhdu",
            573347465, 0.4832633679317633, 6343599562721743399L, "yyhjbva", 708046553699273318L);

        ins(362, PK0_B,PK1_B,PK2_B,PK3_B,PK4_B,PK5_B,PK6_B,
            9022428633928934563L, 0.56795704f, UUID.fromString("39e186c2-c8f7-487a-920a-6a3a7f8d0361"),
            9003116159877849540L, 3458561709672412820L, 0.6791694292210128, 993608413089858709L,
            -1993561279034204629L, 119534805, "lqjxj",
            -2086869212, 0.7183761670474449, 4408177692550082292L, "berbs", 4047523402983762181L);

        ins(366, PK0_A,PK1_A,PK2_A,PK3_A,PK4_A,PK5_A,PK6_A,
            9069551782509199883L, 0.108139575f, UUID.fromString("a8a7e3ed-2b4d-479e-920a-5bbc46d553e2"),
            4302223178633353233L, 7823483121157046262L, 0.16550559082599003, 2610708278172503123L,
            -2223299001641782934L, 343457182, "lidwebtjn",
            -1584427952, 0.21013736125081195, 8227581678036391293L, "kejdl", 2603829970740858348L);

        flush(); // → pa-9

        ins(370, PK0_E,PK1_E,PK2_E,PK3_E,PK4_E,PK5_E,PK6_E,
            7214863582823145493L, 0.41309065f, UUID.fromString("f05b0d4c-38e5-473c-b03c-89eff226c7b6"),
            6366608626036592077L, 2818853087421349200L, 0.1312027296922068, 8624041001808164806L,
            1515344452080497844L, -626853592, "kcwedwquy",
            354657494, 0.3249073759112271, 8762018365937927859L, "goxvdmuvg", 5682306938430493007L);

        ins(371, PK0_B,PK1_B,PK2_B,PK3_B,PK4_B,PK5_B,PK6_B,
            8068888305962221487L, 0.71878743f, UUID.fromString("b6aa919f-3b4a-4718-8559-0bc231f6a1f5"),
            4175554469274387962L, 3325823121555814239L, 0.30827012484740146, 1708633450061016010L,
            6403963000336740274L, -876746227, "mjlcko",
            -322788389, 0.47264369137067774, 5187812060189699494L, "kejdl", 1451456299201212059L);

        ins(375, PK0_B,PK1_B,PK2_B,PK3_B,PK4_B,PK5_B,PK6_B,
            5338393896497634678L, 0.37596166f, UUID.fromString("03969fa3-1a28-407b-8765-984bf8936d6a"),
            8306722840534739472L, 6933808710852207622L, 0.1915161146747133, 6850378265283142657L,
            7699940392811086883L, -174315554, "yohvkyyfb",
            -745395633, 0.4832633679317633, 1162293002033985755L, "erxojysku", 5933106231467348101L);

        ins(379, PK0_C,PK1_C,PK2_C,PK3_C,PK4_C,PK5_C,PK6_C,
            6668709495197969234L, 0.7572929f, UUID.fromString("ff5695f4-c409-4d3d-87a3-2b7d56ce2a79"),
            4533561036270685103L, 4001433606903240362L, 0.41279074997146703, 4167647728039974949L,
            8592874341579893500L, 666861738, "ppnuqtkp",
            2031670199, 0.9246804071557982, 4131234999393286300L, "ispbaaof", 6981052476556420874L);

        ins(385, PK0_E,PK1_E,PK2_E,PK3_E,PK4_E,PK5_E,PK6_E,
            1296080472623078272L, 0.547035f, UUID.fromString("efa0e319-9735-4c80-b295-42a067f5eabe"),
            6596501982232982068L, 7110419925013858804L, 0.5459652331193541, 1641133290687885470L,
            -5586975505643617476L, -1270126615, "ekotju",
            -2056291664, 0.279047802612038, 50087924937296155L, "wftic", 5933106231467348101L);

        ins(387, PK0_E,PK1_E,PK2_E,PK3_E,PK4_E,PK5_E,PK6_E,
            8841421967438068920L, 0.48044294f, UUID.fromString("3de42921-4b7b-4a7d-89c4-aa741edd9d7a"),
            6614400897309150354L, 4988719625721775423L, 0.5840647488049657, 1466717224198142460L,
            -2897961170603275922L, 1550087620, "avdwpwt",
            -2055117706, 0.9392328662103486, 5187812060189699494L, "gvcscpt", 2237846895821823134L);

        ins(388, PK0_D,PK1_D,PK2_D,PK3_D,PK4_D,PK5_D,PK6_D,
            6668709495197969234L, 0.7572929f, UUID.fromString("ff5695f4-c409-4d3d-87a3-2b7d56ce2a79"),
            4533561036270685103L, 4001433606903240362L, 0.41279074997146703, 4167647728039974949L,
            -195839596581881022L, -2124019854, "reiximiur",
            -570266057, 0.8740897098757505, 8762018365937927859L, "nrgtm", 3447737325819254910L);

        execute("DELETE FROM %s USING TIMESTAMP 389 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2=? AND ck3 > ?",
                PK0_C,PK1_C,PK2_C,PK3_C,PK4_C,PK5_C,PK6_C,
                new Date(812178380755637788L), 0.19954473f,
                UUID.fromString("2d3bcd4c-5591-462c-8530-825d8d24fe32"),
                new Date(2817431207435594713L));

        ins(392, PK0_C,PK1_C,PK2_C,PK3_C,PK4_C,PK5_C,PK6_C,
            595951041590324284L, 0.6913666f, UUID.fromString("98bdf92a-b2f0-41b5-a879-c8ab85fe3cdc"),
            60048656530785406L, 7997193456368119895L, 0.07353695608046018, 992606046318140761L,
            2449032612284607258L, -1166959004, "reframawc",
            829978010, 0.7431987652323029, 5309443160979456609L, "wphnqwys", 290180390720296475L);

        execute("DELETE FROM %s USING TIMESTAMP 393 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2 > ? AND ck2 < ?",
                PK0_A,PK1_A,PK2_A,PK3_A,PK4_A,PK5_A,PK6_A,
                new Date(1392851131250887524L), 0.4321807f,
                UUID.fromString("b01a2036-1086-4434-b9d2-bd5bfff8c516"),
                UUID.fromString("625bf161-8b9a-4835-a3f7-fbf988f91caa"));

        execute("DELETE FROM %s USING TIMESTAMP 394 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2 > ? AND ck2 < ?",
                PK0_D,PK1_D,PK2_D,PK3_D,PK4_D,PK5_D,PK6_D,
                new Date(7983587693192248130L), 0.1913073f,
                UUID.fromString("c794be53-5943-4399-b135-eca7bd40b38e"),
                UUID.fromString("c794be53-5943-4399-b135-eca7bd40b38e"));

        execute("DELETE FROM %s USING TIMESTAMP 399 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4=? AND ck5=? AND ck6 >= ?",
                PK0_C,PK1_C,PK2_C,PK3_C,PK4_C,PK5_C,PK6_C,
                new Date(6500789125154428590L), 0.24412262f,
                UUID.fromString("90bdaef3-a9cd-41d2-9534-a86860403996"),
                new Date(7177754381874279647L), new Date(1100329779218904869L),
                0.7806911815651276, new Date(4058285502384290906L));

        execute("DELETE FROM %s USING TIMESTAMP 402 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4 <= ?",
                PK0_B,PK1_B,PK2_B,PK3_B,PK4_B,PK5_B,PK6_B,
                new Date(1296080472623078272L), 0.547035f,
                UUID.fromString("efa0e319-9735-4c80-b295-42a067f5eabe"),
                new Date(6596501982232982068L), new Date(7110419925013858804L));

        ins(403, PK0_D,PK1_D,PK2_D,PK3_D,PK4_D,PK5_D,PK6_D,
            1079028068974742532L, 0.5612422f, UUID.fromString("062bf748-76cb-49a5-a683-f309be30e3b1"),
            1860011166045514480L, 1698899552302820345L, 0.7336355420239814, 6604622076713090327L,
            3357339596975630851L, 1401208238, "enmqootg",
            173895394, 0.3543601400342732, 8505481535598487578L, "kalkbma", 4866832011845139041L);

        ins(405, PK0_A,PK1_A,PK2_A,PK3_A,PK4_A,PK5_A,PK6_A,
            8841421967438068920L, 0.48044294f, UUID.fromString("3de42921-4b7b-4a7d-89c4-aa741edd9d7a"),
            6614400897309150354L, 4988719625721775423L, 0.5840647488049657, 1466717224198142460L,
            7320854646722498592L, -1942257080, "akfbiene",
            195903575, 0.3014857733966231, 418623033704222006L, "waxilwqrh", 9211483162482858808L);

        execute("DELETE FROM %s USING TIMESTAMP 408 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4=? AND ck5=? AND ck6=?",
                PK0_D,PK1_D,PK2_D,PK3_D,PK4_D,PK5_D,PK6_D,
                new Date(3154131362005605271L), 0.50773f,
                UUID.fromString("e8b162f0-bf49-45f5-a555-229071f971ce"),
                new Date(5680709073443119787L), new Date(1209459349905189341L),
                0.10511216164678161, new Date(6094843413164142625L));

        ins(411, PK0_C,PK1_C,PK2_C,PK3_C,PK4_C,PK5_C,PK6_C,
            31575871650152433L, 0.6400059f, UUID.fromString("59f1295e-9b91-408c-a831-1f4ce979ed47"),
            1575901953351712599L, 8231099604976814110L, 0.6311777133703499, 1495447381079163421L,
            6292988200956578332L, 1446601079, "plgiegv",
            -1926612682, 0.8740897098757505, 4670305242640454416L, "evtfaxd", 4112706249927085196L);

        ins(412, PK0_E,PK1_E,PK2_E,PK3_E,PK4_E,PK5_E,PK6_E,
            7732277659165339601L, 0.07786834f, UUID.fromString("74e464ad-b7fb-45ff-87af-0577a000515c"),
            4051168426074289602L, 8599079740564042389L, 0.18594437975524103, 5015712170813548461L,
            6447154842953104728L, 993970894, "gajji",
            411986237, 0.07822641177820588, 359877360643733809L, "hqpoh", 4495381442813990057L);

        ins(414, PK0_D,PK1_D,PK2_D,PK3_D,PK4_D,PK5_D,PK6_D,
            5313340105394188214L, 0.67673683f, UUID.fromString("41142235-7e2a-4cc6-89f1-082b3e4ca9e3"),
            2145587868427495618L, 7770461237366671255L, 0.3548105238997138, 7086297271577062651L,
            7580566413368528364L, -469958493, "vlcqkbld",
            1087335214, 0.5085083634146282, 5497038450726882035L, "puufn", 8076544602803156902L);

        ins(416, PK0_B,PK1_B,PK2_B,PK3_B,PK4_B,PK5_B,PK6_B,
            289175317623305456L, 0.011407256f, UUID.fromString("c4f77de9-e858-4fc6-94aa-f645bd920a5d"),
            4509012127155665723L, 8828609154317613217L, 0.8476855625149918, 8563566072508362377L,
            6235230387129861794L, -469958493, "alhvhk",
            1641153016, 0.2932760017498859, 8302072647452597106L, "xnkylhdy", 6598314740372474360L);

        ins(419, PK0_C,PK1_C,PK2_C,PK3_C,PK4_C,PK5_C,PK6_C,
            7214863582823145493L, 0.41309065f, UUID.fromString("f05b0d4c-38e5-473c-b03c-89eff226c7b6"),
            6366608626036592077L, 2818853087421349200L, 0.1312027296922068, 8624041001808164806L,
            5009396114413956847L, -1177991472, "oagjywki",
            -1117860561, 0.8189672301048969, 636121876247900764L, "fexaisff", 708046553699273318L);

        ins(422, PK0_E,PK1_E,PK2_E,PK3_E,PK4_E,PK5_E,PK6_E,
            174574712713161833L, 0.75370234f, UUID.fromString("86a95a5e-ddc2-462e-9c9a-47978363af0e"),
            3838870093708750296L, 3645877757696077184L, 0.9150718571135844, 4906952502847685337L,
            3357339596975630851L, -506938268, "hxrfdwp",
            -1117860561, 0.6455230265926899, 7313203122061213816L, "anphtrnoq", 2085124337011873187L);

        ins(424, PK0_C,PK1_C,PK2_C,PK3_C,PK4_C,PK5_C,PK6_C,
            1296080472623078272L, 0.547035f, UUID.fromString("efa0e319-9735-4c80-b295-42a067f5eabe"),
            6596501982232982068L, 7110419925013858804L, 0.5459652331193541, 1641133290687885470L,
            8793748800590335815L, -851040014, "huebmq",
            1066524701, 0.8959792116127739, 4387974667178108118L, "qhfgkl", 1014979746352894530L);

        ins(425, PK0_E,PK1_E,PK2_E,PK3_E,PK4_E,PK5_E,PK6_E,
            3925694749834085491L, 0.23965544f, UUID.fromString("0da71f56-e381-4718-94f7-8ab392ea8ee3"),
            2628276105709150041L, 989286681638776389L, 0.41973509681838705, 7307519637770094001L,
            5939124363484848629L, -2124019854, "gsouc",
            -2048007240, 0.1923595199186906, 5309443160979456609L, "ilhihj", 1017011422685689946L);

        compact();

        // ===== pa-12 =====
        ins(427, PK0_D,PK1_D,PK2_D,PK3_D,PK4_D,PK5_D,PK6_D,
            3097542906811718431L, 0.34555322f, UUID.fromString("ce206765-ebcb-4644-a704-9295c4b4cc5c"),
            6485571002832473405L, 5366803924074360686L, 0.23481937599315805, 4449206729258975303L,
            -5541624110829642265L, -506938268, "kcwedwquy",
            1553090433, 0.7013145260458128, 4462482134782265398L, "apovg", 2479188753372019202L);

        execute("DELETE FROM %s USING TIMESTAMP 429 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0 >= ? AND ck0 < ?",
                PK0_C,PK1_C,PK2_C,PK3_C,PK4_C,PK5_C,PK6_C,
                new Date(7732277659165339601L), new Date(1079028068974742532L));

        execute("DELETE FROM %s USING TIMESTAMP 430 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0 >= ? AND ck0 <= ?",
                PK0_D,PK1_D,PK2_D,PK3_D,PK4_D,PK5_D,PK6_D,
                new Date(289175317623305456L), new Date(9069551782509199883L));

        ins(434, PK0_B,PK1_B,PK2_B,PK3_B,PK4_B,PK5_B,PK6_B,
            9176544342293714393L, 0.28937167f, UUID.fromString("96edd406-a2a2-4118-b625-a6fe026bfda2"),
            4686973822019620312L, 1364819553908822931L, 0.4163774190855736, 8204643129472492639L,
            -6241302487817260552L, -1729577101, "usyvedhw",
            1377907697, 0.525411998893379, 8505481535598487578L, "xcofe", 2080391150464250751L);

        execute("DELETE FROM %s USING TIMESTAMP 437 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4 > ? AND ck4 < ?",
                PK0_B,PK1_B,PK2_B,PK3_B,PK4_B,PK5_B,PK6_B,
                new Date(6500789125154428590L), 0.24412262f,
                UUID.fromString("90bdaef3-a9cd-41d2-9534-a86860403996"),
                new Date(7177754381874279647L), new Date(1100329779218904869L),
                new Date(8231099604976814110L));

        ins(439, PK0_B,PK1_B,PK2_B,PK3_B,PK4_B,PK5_B,PK6_B,
            3663771440737928020L, 0.8861742f, UUID.fromString("8aea71eb-d9b8-40e3-87db-41281b57865e"),
            7559617047240140545L, 9129367344068586710L, 0.586569089666871, 6752871757960909080L,
            -3239472837282593694L, -185154532, "ptnpc",
            -712366567, 0.1923595199186906, 8996717975991792391L, "kalkbma", 7002741525609526597L);

        ins(440, PK0_C,PK1_C,PK2_C,PK3_C,PK4_C,PK5_C,PK6_C,
            2683398506297989715L, 0.11806649f, UUID.fromString("3083b908-4d6a-4c25-a637-5c5b03c3a4f5"),
            3439121668211859311L, 3428968279530464015L, 0.058674302466495964, 7173402927535096492L,
            7030263460795444731L, 1700357258, "kfefgoe",
            -533350225, 0.19459268319266543, 803412115264231974L, "knarmvipm", 2479188753372019202L);

        ins(441, PK0_D,PK1_D,PK2_D,PK3_D,PK4_D,PK5_D,PK6_D,
            2153909597307307593L, 0.67634374f, UUID.fromString("63cc83ba-23cf-4099-bddf-d9b0b964877c"),
            4295155149643254177L, 3796559180924064157L, 0.6931094656588638, 4998489496047492772L,
            -2162690793396962771L, 1276453949, "gajji",
            -588896465, 0.615972563879412, 5187812060189699494L, "evtfaxd", 1017011422685689946L);

        // ===== pa-13 =====
        ins(444, PK0_B, PK1_B, PK2_B, PK3_B, PK4_B, PK5_B, PK6_B,
            3154131362005605271L, 0.50773f, UUID.fromString("e8b162f0-bf49-45f5-a555-229071f971ce"),
            5680709073443119787L, 1209459349905189341L, 0.10511216164678161, 6094843413164142625L,
            -5140473047531550406L, -1738283400, "mahsetv",
            -710858132, 0.8829902715954624, 1089629475330235102L, "lkjghx", 3889103742075151192L);

        execute("DELETE FROM %s USING TIMESTAMP 446 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4=? AND ck5=? AND ck6=?",
                PK0_B, PK1_B, PK2_B, PK3_B, PK4_B, PK5_B, PK6_B,
                new Date(6558654313860061088L), 0.6937548f,
                UUID.fromString("d67e05ec-3efc-418a-9c9d-0a612f872972"),
                new Date(3895858524158935411L), new Date(2337806558787240562L),
                0.3789469250433619, new Date(872532026029152686L));

        ins(451, PK0_E, PK1_E, PK2_E, PK3_E, PK4_E, PK5_E, PK6_E,
            6668709495197969234L, 0.7572929f, UUID.fromString("ff5695f4-c409-4d3d-87a3-2b7d56ce2a79"),
            4533561036270685103L, 4001433606903240362L, 0.41279074997146703, 4167647728039974949L,
            718223932867900956L, 601154184, "xdwdxdygl",
            -955398853, 0.7056614349405654, 7153280641827589874L, "uwmiwm", 3793767186743223272L);

        ins(454, PK0_E, PK1_E, PK2_E, PK3_E, PK4_E, PK5_E, PK6_E,
            4800135805541885690L, 0.668106f, UUID.fromString("b5269ba9-38ae-42e1-87f7-e3ef3f7aafd1"),
            1043303864092184203L, 3387188141348146810L, 0.8603391792624008, 2478863737136921293L,
            6235230387129861794L, 1371951603, "dmiitucqo",
            1209671553, 0.8779346958528811, 418623033704222006L, "rrpopk", 3858616978745197017L);

        execute("DELETE FROM %s USING TIMESTAMP 457 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4=? AND ck5=? AND ck6=?",
                PK0_C, PK1_C, PK2_C, PK3_C, PK4_C, PK5_C, PK6_C,
                new Date(6068923614616941042L), 0.29028684f,
                UUID.fromString("9125360e-5906-4012-8dcd-b823aa0996ca"),
                new Date(3641578401446478330L), new Date(4472680207063240628L),
                0.13709918747611993, new Date(24743504383111029L));

        ins(458, PK0_E, PK1_E, PK2_E, PK3_E, PK4_E, PK5_E, PK6_E,
            1079028068974742532L, 0.5612422f, UUID.fromString("062bf748-76cb-49a5-a683-f309be30e3b1"),
            1860011166045514480L, 1698899552302820345L, 0.7336355420239814, 6604622076713090327L,
            6551787366311929840L, 1755569234, "gajji",
            148954010, 0.8779346958528811, 2713610434292900120L, "bkhaj", 7002741525609526597L);

        execute("DELETE FROM %s USING TIMESTAMP 459 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0 <= ?",
                PK0_E, PK1_E, PK2_E, PK3_E, PK4_E, PK5_E, PK6_E,
                new Date(8089855191693757346L));

        ins(462, PK0_E, PK1_E, PK2_E, PK3_E, PK4_E, PK5_E, PK6_E,
            2116382704478849338L, 0.3821351f, UUID.fromString("379e1dae-d51d-43a3-9c45-10a4e5d3c4a1"),
            4104501169326234057L, 8410176165641704849L, 0.03622391957351623, 3578776245965168685L,
            -8340538741053109822L, 2060848501, "ajjdj",
            173470026, 0.9246804071557982, 3408859610715304406L, "holur", 8139221536202086696L);

        ins(463, PK0_D, PK1_D, PK2_D, PK3_D, PK4_D, PK5_D, PK6_D,
            7983587693192248130L, 0.1913073f, UUID.fromString("c794be53-5943-4399-b135-eca7bd40b38e"),
            7449952831176455238L, 7480853520847543271L, 0.5449490541172854, 2989961733792502196L,
            251707020031000675L, -1331346771, "xfthqk",
            329664441, 0.8740897098757505, 4131234999393286300L, "bncqg", 8104235012848134532L);

        ins(467, PK0_C, PK1_C, PK2_C, PK3_C, PK4_C, PK5_C, PK6_C,
            812178380755637788L, 0.19954473f, UUID.fromString("2d3bcd4c-5591-462c-8530-825d8d24fe32"),
            2817431207435594713L, 5879884394154861614L, 0.9041778226943669, 6962938528450013746L,
            -4759817800002031833L, 2029360622, "jtsjb",
            524217192, 0.9685315953315764, 418623033704222006L, "njysbm", 8029524648915357594L);

        ins(478, PK0_E, PK1_E, PK2_E, PK3_E, PK4_E, PK5_E, PK6_E,
            7030544561465643383L, 0.910579f, UUID.fromString("41843ccb-3c51-4026-a191-6e799a78800c"),
            152217791299011442L, 3487853184709005818L, 0.5904247670813827, 3454543472145524600L,
            -2824096784937477989L, 1550087620, "jsxnptfq",
            -533350225, 0.910269862523838, 4387974667178108118L, "ispbaaof", 4319177969471782028L);

        ins(480, PK0_B, PK1_B, PK2_B, PK3_B, PK4_B, PK5_B, PK6_B,
            3925694749834085491L, 0.23965544f, UUID.fromString("0da71f56-e381-4718-94f7-8ab392ea8ee3"),
            2628276105709150041L, 989286681638776389L, 0.41973509681838705, 7307519637770094001L,
            2806453938948825938L, 903347436, "sbkherf",
            329664441, 0.19704888007978127, 4387974667178108118L, "usnvtdse", 3153921970439125085L);

        execute("DELETE FROM %s USING TIMESTAMP 485 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4=? AND ck5 <= ?",
                PK0_D, PK1_D, PK2_D, PK3_D, PK4_D, PK5_D, PK6_D,
                new Date(1198555696041864807L), 0.05360371f,
                UUID.fromString("a3194b6c-dcad-45c4-bcfd-ce7b1166f9f9"),
                new Date(3567313105247075523L), new Date(9030161708007871151L),
                0.24424577436254813);

        ins(488, PK0_D, PK1_D, PK2_D, PK3_D, PK4_D, PK5_D, PK6_D,
            9176544342293714393L, 0.28937167f, UUID.fromString("96edd406-a2a2-4118-b625-a6fe026bfda2"),
            4686973822019620312L, 1364819553908822931L, 0.4163774190855736, 8204643129472492639L,
            -4346382765124629944L, -626853592, "iagqjsup",
            1560267464, 0.3206979518177019, 2935710107150620926L, "wwtctnpmw", 93745134400396197L);

        ins(490, PK0_A, PK1_A, PK2_A, PK3_A, PK4_A, PK5_A, PK6_A,
            2809388297190438194L, 0.58842385f, UUID.fromString("8995c669-7013-494b-8eaa-71123bc163e3"),
            4505845769425413445L, 2380943868211379170L, 0.3913491188570952, 7158536575603078727L,
            958757422847071162L, 833049355, "amicumr",
            -1325411464, 0.13449966921610157, 5578786084678969460L, "usnvtdse", 9037213767141122493L);

        execute("DELETE FROM %s USING TIMESTAMP 493 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4=? AND ck5=? AND ck6=?",
                PK0_A, PK1_A, PK2_A, PK3_A, PK4_A, PK5_A, PK6_A,
                new Date(7951563981932272765L), 0.33266354f,
                UUID.fromString("6b33b392-e435-4193-aab9-aa045332312c"),
                new Date(7132496354360776139L), new Date(3027207019475536322L),
                0.5394878696699469, new Date(1712126919758621283L));

        execute("DELETE FROM %s USING TIMESTAMP 494 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4=? AND ck5=? AND ck6=?",
                PK0_A, PK1_A, PK2_A, PK3_A, PK4_A, PK5_A, PK6_A,
                new Date(174574712713161833L), 0.75370234f,
                UUID.fromString("86a95a5e-ddc2-462e-9c9a-47978363af0e"),
                new Date(3838870093708750296L), new Date(3645877757696077184L),
                0.9150718571135844, new Date(4906952502847685337L));

        ins(497, PK0_B, PK1_B, PK2_B, PK3_B, PK4_B, PK5_B, PK6_B,
            5313340105394188214L, 0.67673683f, UUID.fromString("41142235-7e2a-4cc6-89f1-082b3e4ca9e3"),
            2145587868427495618L, 7770461237366671255L, 0.3548105238997138, 7086297271577062651L,
            -2230461932206953374L, 601154184, "phxguhpg",
            -1117860561, 0.6455230265926899, 5309443160979456609L, "qmufdlwde", 8139221536202086696L);

        ins(504, PK0_A, PK1_A, PK2_A, PK3_A, PK4_A, PK5_A, PK6_A,
            6111890564137593170L, 0.5738175f, UUID.fromString("7ecbd5a0-d306-48b7-b8eb-80aafff55a4f"),
            4897981414968920221L, 8268830877460161659L, 0.780072481444242, 3231222035168309292L,
            3837039669255549446L, 398262149, "xdwdxdygl",
            -588896465, 0.5568927343562914, 1162293002033985755L, "wdadtw", 4463558639681463977L);

        ins(510, PK0_E, PK1_E, PK2_E, PK3_E, PK4_E, PK5_E, PK6_E,
            4537956220047417742L, 0.32449347f, UUID.fromString("1d88fc60-4f10-4d4e-9124-e42042cb1e11"),
            4542789633896780183L, 8874851012397652827L, 0.14732505106046812, 4381764839520968749L,
            1866143645947822265L, -385976444, "kxukmxrib",
            354657494, 0.5515682331226082, 6662105032484961813L, "apovg", 3340887821828193566L);

        execute("DELETE FROM %s USING TIMESTAMP 512 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4=? AND ck5=? AND ck6=?",
                PK0_D, PK1_D, PK2_D, PK3_D, PK4_D, PK5_D, PK6_D,
                new Date(2809388297190438194L), 0.58842385f,
                UUID.fromString("8995c669-7013-494b-8eaa-71123bc163e3"),
                new Date(4505845769425413445L), new Date(2380943868211379170L),
                0.3913491188570952, new Date(7158536575603078727L));

        execute("DELETE FROM %s USING TIMESTAMP 518 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2 >= ? AND ck2 < ?",
                PK0_E, PK1_E, PK2_E, PK3_E, PK4_E, PK5_E, PK6_E,
                new Date(6068923614616941042L), 0.29028684f,
                UUID.fromString("9125360e-5906-4012-8dcd-b823aa0996ca"),
                UUID.fromString("379e1dae-d51d-43a3-9c45-10a4e5d3c4a1"));

        ins(520, PK0_E, PK1_E, PK2_E, PK3_E, PK4_E, PK5_E, PK6_E,
            6580077920587976138L, 0.6841685f, UUID.fromString("d0d5f016-1191-4db6-9d4e-a9d71c74f3e8"),
            8813708496974588178L, 645323024555260859L, 0.12172058993516122, 4258201052351163754L,
            6403963000336740274L, -1997667016, "alhvhk",
            -533350225, 0.6070449986280896, 5578786084678969460L, "waxilwqrh", 3447737325819254910L);

        // THE TRIGGERING DELETE — open-ended RT on partition C, ck1 >= 0.90358263, no upper bound
        execute("DELETE FROM %s USING TIMESTAMP 523 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1 >= ?",
                PK0_C, PK1_C, PK2_C, PK3_C, PK4_C, PK5_C, PK6_C,
                new Date(895752309745821669L), 0.90358263f);

        ins(526, PK0_E, PK1_E, PK2_E, PK3_E, PK4_E, PK5_E, PK6_E,
            6668709495197969234L, 0.7572929f, UUID.fromString("ff5695f4-c409-4d3d-87a3-2b7d56ce2a79"),
            4533561036270685103L, 4001433606903240362L, 0.41279074997146703, 4167647728039974949L,
            -2897961170603275922L, -1184093305, "ksxtgvgiv",
            2113211474, 0.6232784191635833, 7156904707870505366L, "looiyd", 7003760321273720607L);

        execute("DELETE FROM %s USING TIMESTAMP 529 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4=? AND ck5=? AND ck6 > ? AND ck6 <= ?",
                PK0_D, PK1_D, PK2_D, PK3_D, PK4_D, PK5_D, PK6_D,
                new Date(5050696490356231597L), 0.46738684f,
                UUID.fromString("77e0e4ba-a3ac-4083-8623-beaa54534856"),
                new Date(5432546663549514132L), new Date(5348583332677375243L),
                0.5388119779200036, new Date(649843931699566848L), new Date(5757916812157717748L));

        ins(530, PK0_E, PK1_E, PK2_E, PK3_E, PK4_E, PK5_E, PK6_E,
            5338393896497634678L, 0.37596166f, UUID.fromString("03969fa3-1a28-407b-8765-984bf8936d6a"),
            8306722840534739472L, 6933808710852207622L, 0.1915161146747133, 6850378265283142657L,
            -922860269683984248L, 1977864598, "usyvedhw",
            -936131966, 0.6344972933495974, 5578786084678969460L, "wphnqwys", 7015711960780955191L);

        execute("DELETE FROM %s USING TIMESTAMP 532 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4=? AND ck5=? AND ck6=?",
                PK0_B, PK1_B, PK2_B, PK3_B, PK4_B, PK5_B, PK6_B,
                new Date(1079028068974742532L), 0.5612422f,
                UUID.fromString("062bf748-76cb-49a5-a683-f309be30e3b1"),
                new Date(1860011166045514480L), new Date(1698899552302820345L),
                0.7336355420239814, new Date(6604622076713090327L));

        ins(533, PK0_B, PK1_B, PK2_B, PK3_B, PK4_B, PK5_B, PK6_B,
            7758678017172239715L, 0.94963646f, UUID.fromString("9b9af73f-184f-4925-812b-799be541c3e8"),
            5267737720135318876L, 5737101732580574276L, 0.713422590290843, 1271608747715662812L,
            8592874341579893500L, 1550087620, "plgiegv",
            -1741100011, 0.2932760017498859, 5737826017827183249L, "sdelnms", 6140950176947420189L);

        ins(536, PK0_C, PK1_C, PK2_C, PK3_C, PK4_C, PK5_C, PK6_C,
            2482567508472125935L, 0.5997127f, UUID.fromString("7ed31494-4243-4544-96e0-97f76b543228"),
            8557611565075851071L, 2318261385773055094L, 0.8467894324650492, 4127637482729249104L,
            -2230461932206953374L, -876746227, "gsouc",
            1407015489, 0.14190665524536383, 7571347112083109987L, "ceypduusw", 6216803943481526610L);

        execute("DELETE FROM %s USING TIMESTAMP 546 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2=? AND ck3 < ?",
                PK0_D, PK1_D, PK2_D, PK3_D, PK4_D, PK5_D, PK6_D,
                new Date(2397686068888513619L), 0.79958665f,
                UUID.fromString("0881436f-ae57-44c9-b3b4-aa4db5a7f559"),
                new Date(1977521114278017L));

        execute("DELETE FROM %s USING TIMESTAMP 549 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?",
                PK0_E, PK1_E, PK2_E, PK3_E, PK4_E, PK5_E, PK6_E);

        ins(551, PK0_A, PK1_A, PK2_A, PK3_A, PK4_A, PK5_A, PK6_A,
            1296080472623078272L, 0.547035f, UUID.fromString("efa0e319-9735-4c80-b295-42a067f5eabe"),
            6596501982232982068L, 7110419925013858804L, 0.5459652331193541, 1641133290687885470L,
            6357639742829263452L, -1311450888, "seuvd",
            1140804969, 0.5642163691920405, 2129397348230857988L, "fkhiisyw", 2085124337011873187L);

        execute("DELETE FROM %s USING TIMESTAMP 552 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0 >= ? AND ck0 <= ?",
                PK0_A, PK1_A, PK2_A, PK3_A, PK4_A, PK5_A, PK6_A,
                new Date(5050696490356231597L), new Date(7951563981932272765L));

        execute("DELETE FROM %s USING TIMESTAMP 553 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?",
                PK0_A, PK1_A, PK2_A, PK3_A, PK4_A, PK5_A, PK6_A);

        ins(559, PK0_E, PK1_E, PK2_E, PK3_E, PK4_E, PK5_E, PK6_E,
            5338393896497634678L, 0.37596166f, UUID.fromString("03969fa3-1a28-407b-8765-984bf8936d6a"),
            8306722840534739472L, 6933808710852207622L, 0.1915161146747133, 6850378265283142657L,
            -5140473047531550406L, -1640128622, "seuvd",
            1519761326, 0.31848650189380034, 8302072647452597106L, "fbrnpfo", 4112706249927085196L);

        ins(563, PK0_E, PK1_E, PK2_E, PK3_E, PK4_E, PK5_E, PK6_E,
            2657673935921402657L, 0.83093476f, UUID.fromString("ca8db170-c26b-4b15-a224-247053ff707c"),
            3354894401425866113L, 6954223544573883077L, 0.8752323329537812, 1789240321272708090L,
            8592874341579893500L, 666861738, "csfjrajsj",
            1993394723, 0.5898084591692339, 8762018365937927859L, "cuexbek", 9097280949215358125L);

        flush(); // → pa-11

        // ===== pa-12 =====
        ins(573, PK0_C, PK1_C, PK2_C, PK3_C, PK4_C, PK5_C, PK6_C,
            1198555696041864807L, 0.05360371f, UUID.fromString("a3194b6c-dcad-45c4-bcfd-ce7b1166f9f9"),
            3567313105247075523L, 9030161708007871151L, 0.24424577436254813, 2103808420568878265L,
            8271268748448589678L, -851040014, "lidwebtjn",
            808426127, 0.27837314743398844, 5663584026063849861L, "jnuliso", 7002741525609526597L);

        ins(579, PK0_C, PK1_C, PK2_C, PK3_C, PK4_C, PK5_C, PK6_C,
            6068923614616941042L, 0.29028684f, UUID.fromString("9125360e-5906-4012-8dcd-b823aa0996ca"),
            3641578401446478330L, 4472680207063240628L, 0.13709918747611993, 24743504383111029L,
            370079731347674968L, -1738283400, "didnhgnv",
            -1518748838, 0.525411998893379, 7703198908219742685L, "ceypduusw", 5840883269182147789L);

        ins(582, PK0_A, PK1_A, PK2_A, PK3_A, PK4_A, PK5_A, PK6_A,
            3663771440737928020L, 0.8861742f, UUID.fromString("8aea71eb-d9b8-40e3-87db-41281b57865e"),
            7559617047240140545L, 9129367344068586710L, 0.586569089666871, 6752871757960909080L,
            -7415015963551880534L, -1132297796, "iphurudi",
            1209671553, 0.525411998893379, 4093151711978683679L, "rwmkdamyp", 6981052476556420874L);

        ins(588, PK0_B, PK1_B, PK2_B, PK3_B, PK4_B, PK5_B, PK6_B,
            1255940404591624723L, 0.35399938f, UUID.fromString("7bfc40f2-ba61-4d44-a808-67fd474d121e"),
            3213671399070298611L, 873901181423390753L, 0.8131888683947316, 6210264356441707874L,
            -2824096784937477989L, -1831317936, "nodkvj",
            354657494, 0.4293627865942411, 2664178857018113843L, "gbcana", 4047523402983762181L);

        flush(); // → pa-12

        // ===== pa-13 =====
        ins(591, PK0_A, PK1_A, PK2_A, PK3_A, PK4_A, PK5_A, PK6_A,
            1026122448593234227L, 0.16962183f, UUID.fromString("c697db11-b843-49f1-994d-f4721b2fcfac"),
            2285671487235293577L, 3471857804882515297L, 0.06149699724893187, 7608240857631165279L,
            6551787366311929840L, -363361539, "yohvkyyfb",
            1641153016, 0.3506063690532194, 7153280641827589874L, "wwtctnpmw", 2479188753372019202L);

        ins(593, PK0_C, PK1_C, PK2_C, PK3_C, PK4_C, PK5_C, PK6_C,
            5338393896497634678L, 0.37596166f, UUID.fromString("03969fa3-1a28-407b-8765-984bf8936d6a"),
            8306722840534739472L, 6933808710852207622L, 0.1915161146747133, 6850378265283142657L,
            -309961039168156079L, -927446044, "qtnit",
            2031670199, 0.005054617007972939, 8227581678036391293L, "sjnxx", 7034264759872538228L);

        ins(605, PK0_C, PK1_C, PK2_C, PK3_C, PK4_C, PK5_C, PK6_C,
            8229295995722248342L, 0.5424265f, UUID.fromString("d5de434b-dd4a-4a94-b33d-db9dc07e2f67"),
            3872139198142639730L, 4894301062569488584L, 0.6311220473311565, 139079661256660300L,
            5106292788617055220L, 1700357258, "utwpcbt",
            1519761326, 0.005054617007972939, 6954312532750678385L, "xcofe", 2080391150464250751L);

        flush(); // → pa-13

        // ===== pa-15 =====
        ins(610, PK0_B, PK1_B, PK2_B, PK3_B, PK4_B, PK5_B, PK6_B,
            9176544342293714393L, 0.28937167f, UUID.fromString("96edd406-a2a2-4118-b625-a6fe026bfda2"),
            4686973822019620312L, 1364819553908822931L, 0.4163774190855736, 8204643129472492639L,
            3291187951139489121L, 384983922, "egdnquj",
            -791270996, 0.43358976277505057, 2341581714057430219L, "fkhiisyw", 4270645047119037637L);

        execute("DELETE FROM %s USING TIMESTAMP 612 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4 >= ?",
                PK0_D, PK1_D, PK2_D, PK3_D, PK4_D, PK5_D, PK6_D,
                new Date(7951717091631179666L), 0.4980042f,
                UUID.fromString("6f920f1d-c64c-493e-b616-c5ec5efff9dd"),
                new Date(7125370822648282997L), new Date(778614162346502814L));

        ins(613, PK0_A, PK1_A, PK2_A, PK3_A, PK4_A, PK5_A, PK6_A,
            6879379054633031342L, 0.07995385f, UUID.fromString("2da93a81-44ee-4c56-afc2-b5ade99fdcb6"),
            6516028222968028003L, 5941490224199939093L, 0.17771480072824386, 3053745585669451059L,
            5509216129389942989L, 993970894, "qrtpcpb",
            -1336226763, 0.17476282910720276, 4408177692550082292L, "gyffu", 5933106231467348101L);

        ins(614, PK0_A, PK1_A, PK2_A, PK3_A, PK4_A, PK5_A, PK6_A,
            6558654313860061088L, 0.6937548f, UUID.fromString("d67e05ec-3efc-418a-9c9d-0a612f872972"),
            3895858524158935411L, 2337806558787240562L, 0.3789469250433619, 872532026029152686L,
            1314653296687109142L, 2029360622, "ytmirkvef",
            2017663808, 0.5568927343562914, 3870843709397981037L, "wftic", 2080391150464250751L);

        ins(616, PK0_B, PK1_B, PK2_B, PK3_B, PK4_B, PK5_B, PK6_B,
            174574712713161833L, 0.75370234f, UUID.fromString("86a95a5e-ddc2-462e-9c9a-47978363af0e"),
            3838870093708750296L, 3645877757696077184L, 0.9150718571135844, 4906952502847685337L,
            590741162244199549L, -337800296, "ydycgl",
            411986237, 0.07822641177820588, 8481310332272590357L, "xcofe", 6294895644797398413L);

        ins(619, PK0_E, PK1_E, PK2_E, PK3_E, PK4_E, PK5_E, PK6_E,
            2683398506297989715L, 0.11806649f, UUID.fromString("3083b908-4d6a-4c25-a637-5c5b03c3a4f5"),
            3439121668211859311L, 3428968279530464015L, 0.058674302466495964, 7173402927535096492L,
            251707020031000675L, -851040014, "vywwit",
            -1518748838, 0.060410806018144636, 1307612374198208503L, "lkerlhdr", 4684647293078590328L);

        ins(621, PK0_D, PK1_D, PK2_D, PK3_D, PK4_D, PK5_D, PK6_D,
            4800135805541885690L, 0.668106f, UUID.fromString("b5269ba9-38ae-42e1-87f7-e3ef3f7aafd1"),
            1043303864092184203L, 3387188141348146810L, 0.8603391792624008, 2478863737136921293L,
            -4179670244210246046L, -1311450888, "dxkxwm",
            -533350225, 0.47392851513372436, 8996717975991792391L, "npcqb", 7002741525609526597L);

        ins(622, PK0_B, PK1_B, PK2_B, PK3_B, PK4_B, PK5_B, PK6_B,
            812178380755637788L, 0.19954473f, UUID.fromString("2d3bcd4c-5591-462c-8530-825d8d24fe32"),
            2817431207435594713L, 5879884394154861614L, 0.9041778226943669, 6962938528450013746L,
            6447154842953104728L, -1331346771, "xpmoj",
            1379547678, 0.9642100057576731, 7606634101838204817L, "jbpmqj", 8076544602803156902L);

        ins(623, PK0_E, PK1_E, PK2_E, PK3_E, PK4_E, PK5_E, PK6_E,
            556007714326422029L, 0.11358243f, UUID.fromString("116546ed-91dc-4d06-8a43-019d924d421d"),
            3108570581877658278L, 2562427308773256560L, 0.5614545808657748, 6680081408649237683L,
            5509216129389942989L, -450399956, "uiavmvy",
            -469542028, 0.49770734688869733, 359877360643733809L, "wnnbrhgol", 4945018588128193274L);

        ins(626, PK0_B, PK1_B, PK2_B, PK3_B, PK4_B, PK5_B, PK6_B,
            6580077920587976138L, 0.6841685f, UUID.fromString("d0d5f016-1191-4db6-9d4e-a9d71c74f3e8"),
            8813708496974588178L, 645323024555260859L, 0.12172058993516122, 4258201052351163754L,
            8793748800590335815L, 1700357258, "ptnpc",
            1379849717, 0.707941937286291, 6242287956098769446L, "gtgbr", 3340887821828193566L);

        ins(633, PK0_E, PK1_E, PK2_E, PK3_E, PK4_E, PK5_E, PK6_E,
            9176544342293714393L, 0.28937167f, UUID.fromString("96edd406-a2a2-4118-b625-a6fe026bfda2"),
            4686973822019620312L, 1364819553908822931L, 0.4163774190855736, 8204643129472492639L,
            -3542215332208061904L, 401116626, "dbcdf",
            -2055117706, 0.04740482332154616, 5187812060189699494L, "holur", 6216803943481526610L);

        ins(640, PK0_D, PK1_D, PK2_D, PK3_D, PK4_D, PK5_D, PK6_D,
            1198555696041864807L, 0.05360371f, UUID.fromString("a3194b6c-dcad-45c4-bcfd-ce7b1166f9f9"),
            3567313105247075523L, 9030161708007871151L, 0.24424577436254813, 2103808420568878265L,
            2523477132791281503L, 2029360622, "rdjvgotx",
            -44823653, 0.04740482332154616, 7200766659155901218L, "sxqin", 5805871047278242992L);

        ins(642, PK0_E, PK1_E, PK2_E, PK3_E, PK4_E, PK5_E, PK6_E,
            2244580056279493209L, 0.49552703f, UUID.fromString("a53e305c-1f57-4126-88a5-98202860a7a3"),
            7508209535294221793L, 4908129738684902195L, 0.7311285640105962, 3331524405908450885L,
            -8423184196074482743L, -1782601869, "tstuklwhs",
            -1149603166, 0.06500559203317957, 1682818105736464041L, "odnuvlehb", 6981052476556420874L);

        execute("DELETE FROM %s USING TIMESTAMP 643 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0 < ?",
                PK0_C, PK1_C, PK2_C, PK3_C, PK4_C, PK5_C, PK6_C,
                new Date(5313340105394188214L));

        ins(644, PK0_A, PK1_A, PK2_A, PK3_A, PK4_A, PK5_A, PK6_A,
            5313340105394188214L, 0.67673683f, UUID.fromString("41142235-7e2a-4cc6-89f1-082b3e4ca9e3"),
            2145587868427495618L, 7770461237366671255L, 0.3548105238997138, 7086297271577062651L,
            590741162244199549L, 384983922, "ewfiurubo",
            -1693051261, 0.6554929086211027, 8505481535598487578L, "anphtrnoq", 7732337830741508764L);

        ins(658, PK0_D, PK1_D, PK2_D, PK3_D, PK4_D, PK5_D, PK6_D,
            7904320536738506952L, 0.7215915f, UUID.fromString("8df22414-9039-407e-b1d6-75a4a64ced9b"),
            6712297474629946066L, 2326120225376035450L, 0.2567842676904569, 3602158145734328655L,
            3291187951139489121L, 1004456457, "iwlhwrlyl",
            496306431, 0.31848650189380034, 6607201399522087313L, "abdsf", 2080391150464250751L);

        ins(663, PK0_A, PK1_A, PK2_A, PK3_A, PK4_A, PK5_A, PK6_A,
            1971468881480320306L, 0.81026834f, UUID.fromString("86dc21fe-288a-4cdd-ae98-d010d99ef88b"),
            8129812276225968468L, 3095561002072516857L, 0.2405041587925386, 3592289616400232348L,
            590741162244199549L, 903347436, "iagqjsup",
            329664441, 0.8740897098757505, 2341581714057430219L, "ovmdtw", 3793767186743223272L);

        execute("DELETE FROM %s USING TIMESTAMP 664 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4=? AND ck5=? AND ck6=?",
                PK0_D, PK1_D, PK2_D, PK3_D, PK4_D, PK5_D, PK6_D,
                new Date(5981158943882949611L), 0.468988f,
                UUID.fromString("036ad65f-43db-4249-87b9-85094f3a88dd"),
                new Date(5665422587824094996L), new Date(5323609800137447569L),
                0.04763477823183204, new Date(3516214253880021644L));

        execute("DELETE FROM %s USING TIMESTAMP 670 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4 >= ? AND ck4 < ?",
                PK0_C, PK1_C, PK2_C, PK3_C, PK4_C, PK5_C, PK6_C,
                new Date(2244580056279493209L), 0.49552703f,
                UUID.fromString("a53e305c-1f57-4126-88a5-98202860a7a3"),
                new Date(7508209535294221793L), new Date(4908129738684902195L),
                new Date(3095561002072516857L));

        ins(674, PK0_B, PK1_B, PK2_B, PK3_B, PK4_B, PK5_B, PK6_B,
            3600974177033803582L, 0.40629542f, UUID.fromString("625bf161-8b9a-4835-a3f7-fbf988f91caa"),
            8564578635270351319L, 8251058945866637611L, 0.7064072072318436, 6452972081739118195L,
            -6257638358448985380L, 1108124862, "byefnll",
            -735070882, 0.1368904964631943, 2420289100981503227L, "fexaisff", 8893532095607938940L);

        ins(680, PK0_D, PK1_D, PK2_D, PK3_D, PK4_D, PK5_D, PK6_D,
            1296080472623078272L, 0.547035f, UUID.fromString("efa0e319-9735-4c80-b295-42a067f5eabe"),
            6596501982232982068L, 7110419925013858804L, 0.5459652331193541, 1641133290687885470L,
            -8555707246649183016L, 2073608796, "ajjdj",
            1465837439, 0.3014857733966231, 4408177692550082292L, "lkjghx", 7015711960780955191L);
        
        compact();

        ins(690, PK0_D, PK1_D, PK2_D, PK3_D, PK4_D, PK5_D, PK6_D,
            4181777546326856279L, 0.68629456f, UUID.fromString("9dc378c3-8cfd-4636-b3d3-e3ce296352aa"),
            5606861737812623902L, 1821948933589284884L, 0.7190737329990968, 8638229264507788281L,
            6704897206942157594L, -1311450888, "ninlcb",
            329664441, 0.19459268319266543, 7156904707870505366L, "holur", 9019169100950793846L);

        execute("DELETE FROM %s USING TIMESTAMP 691 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2=? AND ck3 > ? AND ck3 <= ?",
                PK0_C, PK1_C, PK2_C, PK3_C, PK4_C, PK5_C, PK6_C,
                new Date(2809388297190438194L), 0.58842385f,
                UUID.fromString("8995c669-7013-494b-8eaa-71123bc163e3"),
                new Date(4505845769425413445L), new Date(6596501982232982068L));

        ins(692, PK0_B, PK1_B, PK2_B, PK3_B, PK4_B, PK5_B, PK6_B,
            7983587693192248130L, 0.1913073f, UUID.fromString("c794be53-5943-4399-b135-eca7bd40b38e"),
            7449952831176455238L, 7480853520847543271L, 0.5449490541172854, 2989961733792502196L,
            -5540878681384831891L, -1227570460, "egdnquj",
            173470026, 0.9392328662103486, 7375779112322779704L, "ispbaaof", 7732337830741508764L);

        ins(698, PK0_A, PK1_A, PK2_A, PK3_A, PK4_A, PK5_A, PK6_A,
            6111890564137593170L, 0.5738175f, UUID.fromString("7ecbd5a0-d306-48b7-b8eb-80aafff55a4f"),
            4897981414968920221L, 8268830877460161659L, 0.780072481444242, 3231222035168309292L,
            5580848209367617853L, 96597707, "dmiitucqo",
            -1970035686, 0.13449966921610157, 1682818105736464041L, "fbrnpfo", 2034380935387208704L);

        ins(706, PK0_A, PK1_A, PK2_A, PK3_A, PK4_A, PK5_A, PK6_A,
            3235151754271335402L, 0.13368613f, UUID.fromString("b1b4c1ae-0a04-49d7-9d5f-23d70077b50a"),
            5364825071757583699L, 5956849314583722713L, 0.31607461811038184, 6654222012218232137L,
            6013596002619984459L, -794988337, "qrtpcpb",
            195903575, 0.8829902715954624, 4131234999393286300L, "addcjn", 4684647293078590328L);

        ins(708, PK0_A, PK1_A, PK2_A, PK3_A, PK4_A, PK5_A, PK6_A,
            4057746519920827335L, 0.5699378f, UUID.fromString("da85dd5f-b8d1-4919-a220-a24f5f4da476"),
            1556922617641054762L, 8985251458796196883L, 0.6703906842060539, 936047755194417775L,
            5283040871071127324L, -1812963328, "lyxavc",
            -1693051261, 0.17476282910720276, 4298635430105934935L, "sxqin", 4112706249927085196L);

        ins(709, PK0_E, PK1_E, PK2_E, PK3_E, PK4_E, PK5_E, PK6_E,
            3808447273678170459L, 0.4901287f, UUID.fromString("05105fbe-e7d2-4f6d-a0cb-cc15b7d94c9b"),
            625755797515586003L, 6112004361837226390L, 0.6455910094352133, 8937815496860999959L,
            6551787366311929840L, 666861738, "ninlcb",
            -1589414833, 0.14190665524536383, 2460593431135453632L, "nrgtm", 2034380935387208704L);

        ins(713, PK0_C, PK1_C, PK2_C, PK3_C, PK4_C, PK5_C, PK6_C,
            5933238244278998195L, 0.5525186f, UUID.fromString("3f73d34d-f7b0-4297-98c9-36501588c2c8"),
            3265534794813778586L, 6061584621833981114L, 0.36830309326038213, 2927894523922161637L,
            -5586975505643617476L, 780223270, "dmiitucqo",
            654190597, 0.15682093518734375, 1089629475330235102L, "viwkfd", 5903827776186115931L);

        ins(715, PK0_C, PK1_C, PK2_C, PK3_C, PK4_C, PK5_C, PK6_C,
            8154640875635237823L, 0.048169494f, UUID.fromString("bab79e81-e824-4c37-a5a7-f796e6890fd6"),
            6293564583563398520L, 1156441304664637843L, 0.7574712860602678, 8402780122121008721L,
            -9018653237497823430L, -782062293, "eyvrcfwkl",
            2113211474, 0.5719522753012989, 7350807410879730860L, "umnfwhvta", 4866832011845139041L);

        ins(716, PK0_C, PK1_C, PK2_C, PK3_C, PK4_C, PK5_C, PK6_C,
            8068888305962221487L, 0.71878743f, UUID.fromString("b6aa919f-3b4a-4718-8559-0bc231f6a1f5"),
            4175554469274387962L, 3325823121555814239L, 0.30827012484740146, 1708633450061016010L,
            6447154842953104728L, -2124019854, "qpccadory",
            276597449, 0.3543601400342732, 856965029349641147L, "mckuesj", 2898073433738709291L);

        ins(720, PK0_B, PK1_B, PK2_B, PK3_B, PK4_B, PK5_B, PK6_B,
            5981158943882949611L, 0.468988f, UUID.fromString("036ad65f-43db-4249-87b9-85094f3a88dd"),
            5665422587824094996L, 5323609800137447569L, 0.04763477823183204, 3516214253880021644L,
            -6241302487817260552L, -1738283400, "egdnquj",
            -2055117706, 0.05947380400224622, 2176371153412431808L, "xcofe", 9037213767141122493L);

        ins(724, PK0_D, PK1_D, PK2_D, PK3_D, PK4_D, PK5_D, PK6_D,
            7904320536738506952L, 0.7215915f, UUID.fromString("8df22414-9039-407e-b1d6-75a4a64ced9b"),
            6712297474629946066L, 2326120225376035450L, 0.2567842676904569, 3602158145734328655L,
            3357339596975630851L, 1023960532, "yohvkyyfb",
            1166777598, 0.9480580666102559, 6044658503208477311L, "puufn", 2328727396519209324L);

        ins(728, PK0_D, PK1_D, PK2_D, PK3_D, PK4_D, PK5_D, PK6_D,
            1255940404591624723L, 0.35399938f, UUID.fromString("7bfc40f2-ba61-4d44-a808-67fd474d121e"),
            3213671399070298611L, 873901181423390753L, 0.8131888683947316, 6210264356441707874L,
            -6241302487817260552L, -1640128622, "gajji",
            -588896465, 0.7183761670474449, 26169827018591802L, "usnvtdse", 2034380935387208704L);

        execute("DELETE FROM %s USING TIMESTAMP 731 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2 >= ? AND ck2 < ?",
                PK0_B, PK1_B, PK2_B, PK3_B, PK4_B, PK5_B, PK6_B,
                new Date(3712741308243752409L), 0.1464836f,
                UUID.fromString("c6e0fbcf-bddd-44b3-958c-1bc4bf243413"),
                UUID.fromString("d5b0ce07-75e1-4bfb-a085-68c656bf4e9f"));

        ins(736, PK0_A, PK1_A, PK2_A, PK3_A, PK4_A, PK5_A, PK6_A,
            5593685444496815230L, 0.569474f, UUID.fromString("30507668-40bb-4236-8f77-7848b96d9a0f"),
            7946049159553714941L, 2507351037773599144L, 0.6343057726047, 44046689115618320L,
            5939124363484848629L, -1132297796, "jtsjb",
            -76968285, 0.15392949489298968, 7703198908219742685L, "rrpopk", 8812580073689633443L);

        ins(739, PK0_A, PK1_A, PK2_A, PK3_A, PK4_A, PK5_A, PK6_A,
            6558654313860061088L, 0.6937548f, UUID.fromString("d67e05ec-3efc-418a-9c9d-0a612f872972"),
            3895858524158935411L, 2337806558787240562L, 0.3789469250433619, 872532026029152686L,
            718223932867900956L, 1401208238, "qtnit",
            -712366567, 0.3506063690532194, 4408177692550082292L, "usnvtdse", 4155390881488293197L);

        execute("DELETE FROM %s USING TIMESTAMP 740 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4=? AND ck5=? AND ck6 > ? AND ck6 <= ?",
                PK0_E, PK1_E, PK2_E, PK3_E, PK4_E, PK5_E, PK6_E,
                new Date(7720752672133646765L), 0.98094994f,
                UUID.fromString("d82b2a80-dba2-4bbd-86ba-5ddb0ffc9f91"),
                new Date(1537578985926481475L), new Date(3524678674385185987L),
                0.8792233897278756, new Date(6538962615637346674L), new Date(1271608747715662812L));

        execute("DELETE FROM %s USING TIMESTAMP 743 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0 >= ? AND ck0 < ?",
                PK0_E, PK1_E, PK2_E, PK3_E, PK4_E, PK5_E, PK6_E,
                new Date(2153909597307307593L), new Date(2116382704478849338L));

        execute("DELETE FROM %s USING TIMESTAMP 745 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1 >= ? AND ck1 <= ?",
                PK0_C, PK1_C, PK2_C, PK3_C, PK4_C, PK5_C, PK6_C,
                new Date(1459850613332409954L), 0.8594484f, 0.23965544f);

        ins(754, PK0_B, PK1_B, PK2_B, PK3_B, PK4_B, PK5_B, PK6_B,
            2657673935921402657L, 0.83093476f, UUID.fromString("ca8db170-c26b-4b15-a224-247053ff707c"),
            3354894401425866113L, 6954223544573883077L, 0.8752323329537812, 1789240321272708090L,
            5106292788617055220L, -1341206773, "yohvkyyfb",
            1885880434, 0.005054617007972939, 5578786084678969460L, "koaeejbyc", 5682306938430493007L);

        ins(762, PK0_E, PK1_E, PK2_E, PK3_E, PK4_E, PK5_E, PK6_E,
            9022428633928934563L, 0.56795704f, UUID.fromString("39e186c2-c8f7-487a-920a-6a3a7f8d0361"),
            9003116159877849540L, 3458561709672412820L, 0.6791694292210128, 993608413089858709L,
            718223932867900956L, 2060848501, "enmqootg",
            -197149523, 0.0026376379381700676, 3151975458783324981L, "uwmiwm", 8076544602803156902L);

        ins(767, PK0_C, PK1_C, PK2_C, PK3_C, PK4_C, PK5_C, PK6_C,
            433951279863394948L, 0.19177824f, UUID.fromString("e0897671-2526-4dcc-a4d0-7dc3d1940b0c"),
            51138102149633332L, 5704788164694372751L, 0.7970479341209122, 7184351183242123477L,
            3837039669255549446L, 1446601079, "gajji",
            1553090433, 0.8740897098757505, 8413680767948360726L, "apovg", 4047523402983762181L);

        ins(769, PK0_B, PK1_B, PK2_B, PK3_B, PK4_B, PK5_B, PK6_B,
            5504845159324637795L, 0.713583f, UUID.fromString("2542aa19-7746-49a7-97e9-52be150a4add"),
            8620805869025402241L, 515169708684029143L, 0.46098040746372493, 6857098616492110070L,
            -3239472837282593694L, 780223270, "npwbd",
            -172401115, 0.615972563879412, 803412115264231974L, "addcjn", 3858616978745197017L);

        execute("DELETE FROM %s USING TIMESTAMP 770 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2=? AND ck3 >= ? AND ck3 <= ?",
                PK0_C, PK1_C, PK2_C, PK3_C, PK4_C, PK5_C, PK6_C,
                new Date(7983587693192248130L), 0.1913073f,
                UUID.fromString("c794be53-5943-4399-b135-eca7bd40b38e"),
                new Date(7449952831176455238L), new Date(9003116159877849540L));

        ins(773, PK0_D, PK1_D, PK2_D, PK3_D, PK4_D, PK5_D, PK6_D,
            6668709495197969234L, 0.7572929f, UUID.fromString("ff5695f4-c409-4d3d-87a3-2b7d56ce2a79"),
            4533561036270685103L, 4001433606903240362L, 0.41279074997146703, 4167647728039974949L,
            -922860269683984248L, 1187403325, "enmqootg",
            -1336226763, 0.36945764555467586, 6954312532750678385L, "sjnxx", 845772459484244853L);

        flush();

        execute("DELETE FROM %s USING TIMESTAMP 778 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2=? AND ck3 >= ? AND ck3 <= ?",
                PK0_D, PK1_D, PK2_D, PK3_D, PK4_D, PK5_D, PK6_D,
                new Date(6926015014934795422L), 0.63504386f,
                UUID.fromString("b05edcf8-ad9c-4631-bf65-cc52da5c6e47"),
                new Date(2548640180170412168L), new Date(7876361165301351903L));

        ins(781, PK0_B, PK1_B, PK2_B, PK3_B, PK4_B, PK5_B, PK6_B,
            8089855191693757346L, 0.204027f, UUID.fromString("fc207c79-0185-4175-93f1-995b0664cf81"),
            6820709174589157461L, 2011076679173066352L, 0.22275537525725275, 7590497905193981801L,
            -3239472837282593694L, -2027921997, "anpund",
            -570266057, 0.31848650189380034, 7350807410879730860L, "fexaisff", 4495381442813990057L);

        ins(790, PK0_E, PK1_E, PK2_E, PK3_E, PK4_E, PK5_E, PK6_E,
            7720752672133646765L, 0.98094994f, UUID.fromString("d82b2a80-dba2-4bbd-86ba-5ddb0ffc9f91"),
            1537578985926481475L, 3524678674385185987L, 0.8792233897278756, 6538962615637346674L,
            -2230461932206953374L, 797672156, "gajji",
            181074133, 0.3367086762540705, 6147310493264803437L, "wftic", 8104235012848134532L);

        ins(792, PK0_C, PK1_C, PK2_C, PK3_C, PK4_C, PK5_C, PK6_C,
            7983587693192248130L, 0.1913073f, UUID.fromString("c794be53-5943-4399-b135-eca7bd40b38e"),
            7449952831176455238L, 7480853520847543271L, 0.5449490541172854, 2989961733792502196L,
            -5344187084142732634L, -2008642336, "ekotju",
            1388470760, 0.2932760017498859, 806659023770351125L, "kjryof", 2603829970740858348L);

        execute("DELETE FROM %s USING TIMESTAMP 793 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4=? AND ck5=? AND ck6=?",
                PK0_A, PK1_A, PK2_A, PK3_A, PK4_A, PK5_A, PK6_A,
                new Date(5981158943882949611L), 0.468988f,
                UUID.fromString("036ad65f-43db-4249-87b9-85094f3a88dd"),
                new Date(5665422587824094996L), new Date(5323609800137447569L),
                0.04763477823183204, new Date(3516214253880021644L));

        ins(795, PK0_E, PK1_E, PK2_E, PK3_E, PK4_E, PK5_E, PK6_E,
            1626372733348952644L, 0.16604358f, UUID.fromString("a0b061a6-b217-45f1-ba03-7d1490746eae"),
            7876361165301351903L, 4792379229547097395L, 0.7911937998839359, 2045208215920818892L,
            -5885890536825904287L, 1423657363, "npwbd",
            1675762480, 0.27837314743398844, 359877360643733809L, "lbyfhoq", 5903827776186115931L);

        ins(800, PK0_E, PK1_E, PK2_E, PK3_E, PK4_E, PK5_E, PK6_E,
            5933238244278998195L, 0.5525186f, UUID.fromString("3f73d34d-f7b0-4297-98c9-36501588c2c8"),
            3265534794813778586L, 6061584621833981114L, 0.36830309326038213, 2927894523922161637L,
            5509216129389942989L, 343457182, "jrwygw",
            -1970035686, 0.9685315953315764, 6176885477697750333L, "wphnqwys", 3889103742075151192L);

        ins(803, PK0_E, PK1_E, PK2_E, PK3_E, PK4_E, PK5_E, PK6_E,
            7030544561465643383L, 0.910579f, UUID.fromString("41843ccb-3c51-4026-a191-6e799a78800c"),
            152217791299011442L, 3487853184709005818L, 0.5904247670813827, 3454543472145524600L,
            5580848209367617853L, -1556583767, "ptnpc",
            -791270996, 0.5500661433421057, 6176885477697750333L, "ovmdtw", 4047523402983762181L);

        execute("DELETE FROM %s USING TIMESTAMP 804 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0 > ? AND ck0 <= ?",
                PK0_B, PK1_B, PK2_B, PK3_B, PK4_B, PK5_B, PK6_B,
                new Date(1296080472623078272L), new Date(9069551782509199883L));

        execute("DELETE FROM %s USING TIMESTAMP 810 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4=? AND ck5 > ?",
                PK0_C, PK1_C, PK2_C, PK3_C, PK4_C, PK5_C, PK6_C,
                new Date(4800135805541885690L), 0.668106f,
                UUID.fromString("b5269ba9-38ae-42e1-87f7-e3ef3f7aafd1"),
                new Date(1043303864092184203L), new Date(3387188141348146810L),
                0.8603391792624008);

        ins(811, PK0_D, PK1_D, PK2_D, PK3_D, PK4_D, PK5_D, PK6_D,
            6879379054633031342L, 0.07995385f, UUID.fromString("2da93a81-44ee-4c56-afc2-b5ade99fdcb6"),
            6516028222968028003L, 5941490224199939093L, 0.17771480072824386, 3053745585669451059L,
            590741162244199549L, -337800296, "sdouexqn",
            2031670199, 0.5898084591692339, 788526055782773280L, "qnvyv", 3752168384871718894L);

        ins(822, PK0_B, PK1_B, PK2_B, PK3_B, PK4_B, PK5_B, PK6_B,
            6558654313860061088L, 0.6937548f, UUID.fromString("d67e05ec-3efc-418a-9c9d-0a612f872972"),
            3895858524158935411L, 2337806558787240562L, 0.3789469250433619, 872532026029152686L,
            3357339596975630851L, 797672156, "huebmq",
            -1260453690, 0.3561561191913202, 3567305114876562776L, "hqpoh", 1451456299201212059L);

        execute("DELETE FROM %s USING TIMESTAMP 823 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4=? AND ck5 >= ? AND ck5 < ?",
                PK0_B, PK1_B, PK2_B, PK3_B, PK4_B, PK5_B, PK6_B,
                new Date(6111890564137593170L), 0.5738175f,
                UUID.fromString("7ecbd5a0-d306-48b7-b8eb-80aafff55a4f"),
                new Date(4897981414968920221L), new Date(8268830877460161659L),
                0.780072481444242, 0.5900811957624077);

        flush();

        execute("DELETE FROM %s USING TIMESTAMP 825 WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=?" +
                " AND ck0=? AND ck1=? AND ck2=? AND ck3=? AND ck4=? AND ck5=? AND ck6=?",
                PK0_A, PK1_A, PK2_A, PK3_A, PK4_A, PK5_A, PK6_A,
                new Date(8339532628757719152L), 0.1642788f,
                UUID.fromString("7430bc2b-a649-42a1-8ead-7b00ac6a22e3"),
                new Date(7233124093057588305L), new Date(4981386253553991482L),
                0.562929033463869, new Date(381823169738951048L));
        
        compact();

        ins(829, PK0_C, PK1_C, PK2_C, PK3_C, PK4_C, PK5_C, PK6_C,
            2116382704478849338L, 0.3821351f, UUID.fromString("379e1dae-d51d-43a3-9c45-10a4e5d3c4a1"),
            4104501169326234057L, 8410176165641704849L, 0.03622391957351623, 3578776245965168685L,
            5580848209367617853L, 903347436, "aqhaay",
            2049236113, 0.2367592909929076, 7746022797742066891L, "wftic", 5840883269182147789L);

        ins(832, PK0_E, PK1_E, PK2_E, PK3_E, PK4_E, PK5_E, PK6_E,
            2566152003571977594L, 0.5282622f, UUID.fromString("5247fabe-c3a6-4a28-b779-c1925aa94fc7"),
            4180268629224916553L, 4264960205105382903L, 0.6679116538607843, 7363251670154659579L,
            -2230461932206953374L, 401116626, "ewfiurubo",
            2073276432, 0.8398080869387932, 4462482134782265398L, "sdelnms", 7034264759872538228L);

        execute("SELECT * FROM %s WHERE pk0=? AND pk1=? AND pk2=? AND pk3=? AND pk4=? AND pk5=? AND pk6=? ORDER BY ck0 ASC, ck1 DESC, ck2 DESC, ck3 DESC, ck4 ASC, ck5 ASC, ck6 ASC",
                PK0_C, PK1_C, PK2_C, PK3_C, PK4_C, PK5_C, PK6_C);

    }
}